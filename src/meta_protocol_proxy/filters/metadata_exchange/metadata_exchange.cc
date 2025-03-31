#include "src/meta_protocol_proxy/filters/metadata_exchange/metadata_exchange.h"

#include "src/meta_protocol_proxy/filters/common/base64.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace MetadataExchange {
MetadataExchangeFilter::MetadataExchangeFilter(
    const aeraki::meta_protocol_proxy::filters::metadata_exchange::v1alpha::MetadataExchange&,
    const Server::Configuration::FactoryContext& context) {
  loadMetadataFromNodeInfo(context.serverFactoryContext().localInfo());
  traffic_direction_ = context.listenerInfo().direction();
}

FilterStatus MetadataExchangeFilter::onMessageDecoded(MetadataSharedPtr,
                                                      MutationSharedPtr mutation) {
  // if this is an outbound request, we need to send node metadata to peer
  if (traffic_direction_ == envoy::config::core::v3::TrafficDirection::OUTBOUND) {
    (*mutation.get())[ExchangeMetadataHeader] = local_node_metadata_;
    (*mutation.get())[ExchangeMetadataHeaderId] = local_node_metadata_id_;
  }
  return FilterStatus::ContinueIteration;
}

FilterStatus MetadataExchangeFilter::onMessageEncoded(MetadataSharedPtr,
                                                      MutationSharedPtr mutation) {
  // if this is an inbound request, we need to send node metadata to peer in the response
  if (traffic_direction_ == envoy::config::core::v3::TrafficDirection::INBOUND) {
    (*mutation.get())[ExchangeMetadataHeader] = local_node_metadata_;
    (*mutation.get())[ExchangeMetadataHeaderId] = local_node_metadata_id_;
  }
  return FilterStatus::ContinueIteration;
}

void MetadataExchangeFilter::loadMetadataFromNodeInfo(const LocalInfo::LocalInfo& local_info) {
  if (local_info.node().has_metadata()) {
    const auto obj = Istio::Common::convertStructToWorkloadMetadata(local_info.node().metadata());
    google::protobuf::Struct processed_metadata = Istio::Common::convertWorkloadMetadataToStruct(*obj);
    std::string metadata_bytes;
    google::protobuf::io::StringOutputStream stream(&metadata_bytes); 
    processed_metadata.SerializePartialToZeroCopyStream(&stream);
    local_node_metadata_ = Base64::encode(metadata_bytes.data(), metadata_bytes.size());
  }
  local_node_metadata_id_ = local_info.node().id();
}

} // namespace MetadataExchange
} // namespace  MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
