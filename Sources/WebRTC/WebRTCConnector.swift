import Core
import AVFAudio
import Foundation
import OSLog
@preconcurrency import LiveKitWebRTC
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

@Observable public final class WebRTCConnector: NSObject, RealtimeConnector, Sendable {
	package enum WebRTCError: Error {
		case invalidClientSecret
		case missingAudioPermission
		case failedToCreateDataChannel
		case failedToCreatePeerConnection
		case badServerResponse(statusCode: Int?, body: String?)
		case failedToCreateSDPOffer(Swift.Error)
		case failedToSetLocalDescription(Swift.Error)
		case failedToSetRemoteDescription(Swift.Error)
	}

	package let events: AsyncThrowingStream<ServerEvent, Error>
	package let statusUpdates: AsyncStream<RealtimeAPI.Status>
	@MainActor package private(set) var status = RealtimeAPI.Status.disconnected

	package var isMuted: Bool {
		!audioTrack.isEnabled
	}

	package let audioTrack: LKRTCAudioTrack
	private let dataChannel: LKRTCDataChannel
	private let connection: LKRTCPeerConnection

	private let stream: AsyncThrowingStream<ServerEvent, Error>.Continuation
	private let statusStream: AsyncStream<RealtimeAPI.Status>.Continuation

	private static let factory: LKRTCPeerConnectionFactory = {
		LKRTCInitializeSSL()

		return LKRTCPeerConnectionFactory()
	}()

	private let encoder: JSONEncoder = {
		let encoder = JSONEncoder()
		encoder.keyEncodingStrategy = .convertToSnakeCase
		return encoder
	}()

	private let decoder: JSONDecoder = {
		let decoder = JSONDecoder()
		decoder.keyDecodingStrategy = .convertFromSnakeCase
		return decoder
	}()

	private static let logger = Logger(subsystem: "RealtimeAPI", category: "WebRTCConnector")

	private init(connection: LKRTCPeerConnection, audioTrack: LKRTCAudioTrack, dataChannel: LKRTCDataChannel) {
		self.connection = connection
		self.audioTrack = audioTrack
		self.dataChannel = dataChannel
		(events, stream) = AsyncThrowingStream.makeStream(of: ServerEvent.self)
		(statusUpdates, statusStream) = AsyncStream.makeStream(of: RealtimeAPI.Status.self)

		super.init()

		statusStream.yield(.disconnected)
		connection.delegate = self
		dataChannel.delegate = self
	}

	deinit {
		// Cannot touch @MainActor state (`status`) from nonisolated deinit.
		// Do non-isolated cleanup only. If disconnect() already ran (the normal
		// path), these calls are idempotent no-ops.
		connection.close()
		stream.finish()
		statusStream.finish()
	}

	package func connect(using request: URLRequest, configuration: SessionConfiguration? = nil) async throws {
		guard connection.connectionState == .new else { return }

		guard AVAudioApplication.shared.recordPermission == .granted else {
			throw WebRTCError.missingAudioPermission
		}

		await MainActor.run {
			status = .connecting
			statusStream.yield(.connecting)
		}

		try await performHandshake(using: request, configuration: configuration)
		Self.configureAudioSession()
	}

	package func send(event: ClientEvent) throws {
		try dataChannel.sendData(LKRTCDataBuffer(data: encoder.encode(event), isBinary: false))
	}

	@MainActor
	package func disconnect() {
		status = .disconnected
		statusStream.yield(.disconnected)
		statusStream.finish()
		connection.close()
		stream.finish()
	}

	package func toggleMute() {
		audioTrack.isEnabled.toggle()
	}
}

extension WebRTCConnector {
	package static func create(connectingTo request: URLRequest) async throws -> WebRTCConnector {
		let connector = try create()
		try await connector.connect(using: request)
		return connector
	}

	static func create(connectingTo request: URLRequest, configuration: SessionConfiguration) async throws -> WebRTCConnector {
		let connector = try create()
		try await connector.connect(using: request, configuration: configuration)
		return connector
	}

	package static func create() throws -> WebRTCConnector {
		guard let connection = factory.peerConnection(
			with: LKRTCConfiguration(),
			constraints: LKRTCMediaConstraints(mandatoryConstraints: nil, optionalConstraints: nil),
			delegate: nil
		) else { throw WebRTCError.failedToCreatePeerConnection }

		let audioTrack = Self.setupLocalAudio(for: connection)

		guard let dataChannel = connection.dataChannel(forLabel: "oai-events", configuration: LKRTCDataChannelConfiguration()) else {
			throw WebRTCError.failedToCreateDataChannel
		}

		return self.init(connection: connection, audioTrack: audioTrack, dataChannel: dataChannel)
	}
}

private extension WebRTCConnector {
	static func setupLocalAudio(for connection: LKRTCPeerConnection) -> LKRTCAudioTrack {
		let audioSource = factory.audioSource(with: LKRTCMediaConstraints(
			mandatoryConstraints: [
				"googNoiseSuppression": "true", "googHighpassFilter": "true",
				"googEchoCancellation": "true", "googAutoGainControl": "true",
			],
			optionalConstraints: nil
		))

		return tap(factory.audioTrack(with: audioSource, trackId: "local_audio")) { audioTrack in
			connection.add(audioTrack, streamIds: ["local_stream"])
		}
	}

	static func configureAudioSession() {
		#if !os(macOS)
		do {
			let audioSession = AVAudioSession.sharedInstance()
			#if os(tvOS)
			try audioSession.setCategory(.playAndRecord, options: [])
			#else
			try audioSession.setCategory(.playAndRecord, options: [.defaultToSpeaker])
			#endif
			try audioSession.setMode(.videoChat)
			try audioSession.setActive(true, options: .notifyOthersOnDeactivation)
		} catch {
			logger.error("Failed to configure AVAudioSession: \(String(describing: error), privacy: .public)")
		}
		#endif
	}

	func performHandshake(using request: URLRequest, configuration: SessionConfiguration? = nil) async throws {
		let sdp = try await Result { try await connection.offer(for: LKRTCMediaConstraints(mandatoryConstraints: ["levelControl": "true"], optionalConstraints: nil)) }
			.mapError(WebRTCError.failedToCreateSDPOffer)
			.get()

		do { try await connection.setLocalDescription(sdp) }
		catch { throw WebRTCError.failedToSetLocalDescription(error) }

		let remoteSdp = try await fetchRemoteSDP(using: request, localSdp: connection.localDescription!.sdp, configuration: configuration)

		do { try await connection.setRemoteDescription(LKRTCSessionDescription(type: .answer, sdp: remoteSdp)) }
		catch { throw WebRTCError.failedToSetRemoteDescription(error) }
	}

	func fetchRemoteSDP(using request: URLRequest, localSdp: String, configuration: SessionConfiguration?) async throws -> String {
		var request = request

		if let configuration {
			let boundary = UUID().uuidString
			request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
			request.httpBody = try buildMultipartBody(boundary: boundary, sdp: localSdp, configuration: configuration)
		} else {
			request.httpBody = localSdp.data(using: .utf8)
			request.setValue("application/sdp", forHTTPHeaderField: "Content-Type")
		}

		let (data, response) = try await URLSession.shared.data(for: request)

		guard let response = response as? HTTPURLResponse, response.statusCode == 201, let remoteSdp = String(data: data, encoding: .utf8) else {
			let httpResponse = response as? HTTPURLResponse
			let body = String(data: data, encoding: .utf8)
			if httpResponse?.statusCode == 401 { throw WebRTCError.invalidClientSecret }
			throw WebRTCError.badServerResponse(statusCode: httpResponse?.statusCode, body: body)
		}

		return remoteSdp
	}

	func buildMultipartBody(boundary: String, sdp: String, configuration: SessionConfiguration) throws -> Data {
		let sessionJSON = try encoder.encode(configuration)

		var body = Data()

		body.append("--\(boundary)\r\n")
		body.append("Content-Disposition: form-data; name=\"sdp\"\r\n")
		body.append("Content-Type: application/sdp\r\n\r\n")
		body.append(sdp)
		body.append("\r\n")

		body.append("--\(boundary)\r\n")
		body.append("Content-Disposition: form-data; name=\"session\"\r\n")
		body.append("Content-Type: application/json\r\n\r\n")
		body.append(sessionJSON)
		body.append("\r\n")

		body.append("--\(boundary)--\r\n")

		return body
	}
}

private extension Data {
	mutating func append(_ string: String) {
		if let data = string.data(using: .utf8) {
			append(data)
		}
	}
}

extension WebRTCConnector: LKRTCPeerConnectionDelegate {
	public func peerConnectionShouldNegotiate(_: LKRTCPeerConnection) {}
	public func peerConnection(_: LKRTCPeerConnection, didAdd _: LKRTCMediaStream) {}
	public func peerConnection(_: LKRTCPeerConnection, didOpen _: LKRTCDataChannel) {}
	public func peerConnection(_: LKRTCPeerConnection, didRemove _: LKRTCMediaStream) {}
	public func peerConnection(_: LKRTCPeerConnection, didChange _: LKRTCSignalingState) {}
	public func peerConnection(_: LKRTCPeerConnection, didGenerate _: LKRTCIceCandidate) {}
	public func peerConnection(_: LKRTCPeerConnection, didRemove _: [LKRTCIceCandidate]) {}
	public func peerConnection(_: LKRTCPeerConnection, didChange _: LKRTCIceGatheringState) {}

	public func peerConnection(_: LKRTCPeerConnection, didChange newState: LKRTCIceConnectionState) {
		Self.logger.debug("ICE connection state changed: \(String(describing: newState), privacy: .public)")
	}
}

extension WebRTCConnector: LKRTCDataChannelDelegate {
	public func dataChannel(_: LKRTCDataChannel, didReceiveMessageWith buffer: LKRTCDataBuffer) {
		do { try stream.yield(decoder.decode(ServerEvent.self, from: buffer.data)) }
		catch {
			// OpenAI regularly adds new event types. A single unknown event must not
			// terminate the voice stream — log and skip instead.
			Self.logger.warning("Skipping unrecognized server event: \(error, privacy: .public). Payload (truncated): \(String(data: buffer.data.prefix(300), encoding: .utf8) ?? "<invalid utf8>", privacy: .public)")
		}
	}

	public func dataChannelDidChangeState(_ dataChannel: LKRTCDataChannel) {
		Task { @MainActor [state = dataChannel.readyState] in
			switch state {
				case .open:
					status = .connected
					statusStream.yield(.connected)
				case .closing, .closed:
					status = .disconnected
					statusStream.yield(.disconnected)
					statusStream.finish()
				default: break
			}
		}
	}
}
