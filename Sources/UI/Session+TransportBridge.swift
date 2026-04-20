import Core
import Foundation
import WebSocket
import WebRTC

extension Session {
	func prepareForConnection() {
		guard task == nil || statusTask == nil else { return }

		if let transportFactory {
			let (transport, pendingFailures) = transportFactory()
			self.transport = transport
			for failure in pendingFailures {
				failureStream.yield(failure)
			}
		}

		startRuntimeTasks(using: transport)
		transport.setMuted(muted)
	}

	func startRuntimeTasks(using transport: Transport) {
		task = Self.makeEventTask(transport: transport, handleEvent: { [weak self] event in
			guard let self else { return }
			self.serverEventStream.yield(event)
			try await self.handleEvent(event)
		}, reportFailure: { [weak self] failure in
			guard let self else { return }
			self.failureStream.yield(failure)
		})
		statusTask = Self.makeStatusTask(transport: transport) { [weak self] status in
			guard let self else { return }
			self.handleStatusUpdate(status)
		}
	}

	static func makeDefaultTransport(using connectionTransport: ConnectionTransport) -> (Transport, [SessionError]) {
		switch connectionTransport {
			case .webRTC:
				do {
					return (makeLiveTransport(from: try WebRTCConnector.create()), [])
				} catch {
					let failure = SessionError.connectorInitializationFailed
					return (makeUnavailableTransport(for: failure), [failure])
				}
			case .webSocket:
				return (makeLazyWebSocketTransport(), [])
		}
	}

	static func makeLiveTransport(from client: WebRTCConnector) -> Transport {
		.init(
			events: client.events,
			statusUpdates: client.statusUpdates,
			status: { client.status },
			connect: { request in try await client.connect(using: request) },
			send: { event in try client.send(event: event) },
			disconnect: {
				// `client.disconnect()` is @MainActor-isolated. All callers of
				// `Transport.disconnect` invoke it from MainActor (Session.disconnect()
				// is @MainActor; Session.deinit uses MainActor.assumeIsolated), so we
				// safely assume isolation here to satisfy the nonisolated closure type.
				MainActor.assumeIsolated { client.disconnect() }
			},
			setMuted: { muted in client.audioTrack.isEnabled = !muted }
		)
	}

	static func makeLazyWebSocketTransport() -> Transport {
		let (events, eventContinuation) = AsyncThrowingStream.makeStream(of: ServerEvent.self)
		let (statusUpdates, statusContinuation) = AsyncStream.makeStream(of: RealtimeAPI.Status.self)

		actor State {
			let eventContinuation: AsyncThrowingStream<ServerEvent, Error>.Continuation
			let statusContinuation: AsyncStream<RealtimeAPI.Status>.Continuation
			var connector: WebSocketConnector?
			var eventTask: Task<Void, Never>?
			var statusTask: Task<Void, Never>?

			init(
				eventContinuation: AsyncThrowingStream<ServerEvent, Error>.Continuation,
				statusContinuation: AsyncStream<RealtimeAPI.Status>.Continuation
			) {
				self.eventContinuation = eventContinuation
				self.statusContinuation = statusContinuation
			}

			func connect(using request: URLRequest) async throws {
				guard connector == nil else { return }

				let connector = try await WebSocketConnector.create(connectingTo: request)
				self.connector = connector

				let currentStatus = await MainActor.run { connector.status }
				statusContinuation.yield(currentStatus)

				// Bridge status updates through actor-owned state so the public session
				// never shares mutable transport internals across tasks.
				statusTask = Task {
					for await status in connector.statusUpdates {
						self.yield(status: status)
					}
					self.finishStatusUpdates()
				}

				// Keep the raw event bridge separate from the main runtime task so we can
				// terminate the forwarded stream explicitly when the transport ends.
				eventTask = Task {
					do {
						for try await event in connector.events {
							self.yield(event: event)
						}
						self.finishEvents()
					} catch {
						self.finishEvents(throwing: error)
					}
				}
			}

			func send(event: ClientEvent) async throws {
				guard let connector else { throw SessionError.connectionNotEstablished }
				try await connector.send(event: event)
			}

			func yield(status: RealtimeAPI.Status) {
				statusContinuation.yield(status)
			}

			func finishStatusUpdates() {
				statusContinuation.finish()
			}

			func yield(event: ServerEvent) {
				eventContinuation.yield(event)
			}

			func finishEvents() {
				eventContinuation.finish()
			}

			func finishEvents(throwing error: Error) {
				eventContinuation.finish(throwing: error)
			}

			func disconnect() async {
				if let connector {
					await MainActor.run { connector.disconnect() }
				}
				connector = nil
				eventTask?.cancel()
				statusTask?.cancel()
				eventTask = nil
				statusTask = nil
				statusContinuation.yield(.disconnected)
				statusContinuation.finish()
				eventContinuation.finish()
			}
		}

		let state = State(eventContinuation: eventContinuation, statusContinuation: statusContinuation)

		return .init(
			events: events,
			statusUpdates: statusUpdates,
			status: { .disconnected },
			connect: { request in try await state.connect(using: request) },
			send: { event in try await state.send(event: event) },
			disconnect: { Task { await state.disconnect() } },
			setMuted: { _ in }
		)
	}

	static func makeUnavailableTransport(for failure: SessionError) -> Transport {
		let events = AsyncThrowingStream<ServerEvent, Error> { continuation in
			continuation.finish()
		}
		let statusUpdates = AsyncStream<RealtimeAPI.Status> { continuation in
			continuation.yield(.disconnected)
			continuation.finish()
		}

		return .init(
			events: events,
			statusUpdates: statusUpdates,
			status: { .disconnected },
			connect: { _ in throw failure },
			send: { _ in throw failure },
			disconnect: {},
			setMuted: { _ in }
		)
	}
}
