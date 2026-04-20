import Core
import Foundation
import OSLog
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public final class WebSocketConnector: NSObject, RealtimeConnector, Sendable {
	package let events: AsyncThrowingStream<ServerEvent, Error>
	package let statusUpdates: AsyncStream<RealtimeAPI.Status>
	@MainActor package private(set) var status = RealtimeAPI.Status.connecting

	private static let logger = Logger(subsystem: "RealtimeAPI", category: "WebSocketConnector")

	private let task: Task<Void, Never>
	private let webSocket: URLSessionWebSocketTask
	private let stream: AsyncThrowingStream<ServerEvent, Error>.Continuation
	private let statusStream: AsyncStream<RealtimeAPI.Status>.Continuation

	private let encoder: JSONEncoder = {
		let encoder = JSONEncoder()
		encoder.keyEncodingStrategy = .convertToSnakeCase
		return encoder
	}()

	init(connectingTo request: URLRequest) {
		let (events, stream) = AsyncThrowingStream.makeStream(of: ServerEvent.self)
		let (statusUpdates, statusStream) = AsyncStream.makeStream(of: RealtimeAPI.Status.self)

		let webSocket = URLSession.shared.webSocketTask(with: request)

		self.events = events
		self.statusUpdates = statusUpdates
		self.stream = stream
		self.statusStream = statusStream
		self.webSocket = webSocket

		task = Task.detached { [webSocket, stream] in
			var isActive = true

			let decoder = JSONDecoder()
			decoder.keyDecodingStrategy = .convertFromSnakeCase

			while isActive, webSocket.closeCode == .invalid, !Task.isCancelled {
				guard webSocket.closeCode == .invalid else {
					stream.finish()
					isActive = false
					break
				}

				do {
					let message = try await webSocket.receive()

					guard case let .string(text) = message, let data = text.data(using: .utf8) else {
						stream.finish(throwing: RealtimeAPI.Error.invalidMessage)
						continue
					}

					do {
						try stream.yield(decoder.decode(ServerEvent.self, from: data))
					} catch {
						// OpenAI regularly adds new event types. A single unknown event must not
						// terminate the voice stream — log and skip instead.
						Self.logger.warning("Skipping unrecognized server event: \(error, privacy: .public). Payload (truncated): \(String(data: data.prefix(300), encoding: .utf8) ?? "<invalid utf8>", privacy: .public)")
					}
				} catch {
					stream.finish(throwing: error)
					isActive = false
				}
			}

			webSocket.cancel(with: .goingAway, reason: nil)
		}

		super.init()

		statusStream.yield(.connecting)
		webSocket.delegate = self
		webSocket.resume()
	}

	deinit {
		// disconnect() is @MainActor-isolated and cannot be called from a
		// nonisolated deinit. Do the equivalent non-isolated cleanup inline.
		// If disconnect() already ran (the normal path), these calls are
		// idempotent no-ops.
		statusStream.finish()
		webSocket.cancel(with: .goingAway, reason: nil)
		task.cancel()
		stream.finish()
	}

	package static func create(connectingTo request: URLRequest) async throws -> WebSocketConnector {
		return self.init(connectingTo: request)
	}

	package func send(event: ClientEvent) async throws {
		let message = try URLSessionWebSocketTask.Message.string(String(data: encoder.encode(event), encoding: .utf8)!)
		try await webSocket.send(message)
	}

	@MainActor
	package func disconnect() {
		statusStream.yield(.disconnected)
		statusStream.finish()
		webSocket.cancel(with: .goingAway, reason: nil)
		task.cancel()
		stream.finish()
	}
}

extension WebSocketConnector: URLSessionWebSocketDelegate {
	public func urlSession(_: URLSession, webSocketTask _: URLSessionWebSocketTask, didOpenWithProtocol _: String?) {
		Task { @MainActor in
			status = .connected
			statusStream.yield(.connected)
		}
	}

	public func urlSession(_: URLSession, webSocketTask _: URLSessionWebSocketTask, didCloseWith _: URLSessionWebSocketTask.CloseCode, reason _: Data?) {
		Task { @MainActor in
			status = .disconnected
			statusStream.yield(.disconnected)
			statusStream.finish()
		}
	}
}
