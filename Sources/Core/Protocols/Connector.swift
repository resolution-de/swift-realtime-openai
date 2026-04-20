import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

package protocol RealtimeConnector: Sendable {
	@MainActor var status: RealtimeAPI.Status { get }
	var statusUpdates: AsyncStream<RealtimeAPI.Status> { get }
	var events: AsyncThrowingStream<ServerEvent, Error> { get }

	static func create(connectingTo request: URLRequest) async throws -> Self

	@MainActor func disconnect()
	func send(event: ClientEvent) async throws
}
