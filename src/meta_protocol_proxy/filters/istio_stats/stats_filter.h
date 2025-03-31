#pragma once

// Envoy
#include "envoy/local_info/local_info.h"
#include "envoy/server/factory_context.h"
#include "source/common/common/logger.h"

// istio proxy
#include "api/meta_protocol_proxy/filters/istio_stats/v1alpha/istio_stats.pb.h"
#include "src/meta_protocol_proxy/filters/filter.h"
#include "src/meta_protocol_proxy/filters/istio_stats/istio_stats.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace IstioStats {

const std::string ExchangeMetadataHeader = "x-envoy-peer-metadata";
const std::string ExchangeMetadataHeaderId = "x-envoy-peer-metadata-id";

class StatsFilter : public CodecFilter, Logger::Loggable<Logger::Id::filter> {
public:
  StatsFilter(const aeraki::meta_protocol_proxy::filters::istio_stats::v1alpha::IstioStats&,
              const Server::Configuration::FactoryContext& context, IstioStats& istioStats);
  ~StatsFilter() override = default;
  void onDestroy() override{};

  // DecoderFilter
  void setDecoderFilterCallbacks(DecoderFilterCallbacks&) override{};
  FilterStatus onMessageDecoded(MetadataSharedPtr metadata, MutationSharedPtr mutation) override;

  void setEncoderFilterCallbacks(EncoderFilterCallbacks&) override{};
  FilterStatus onMessageEncoded(MetadataSharedPtr, MutationSharedPtr) override;

private:
  google::protobuf::Struct extractPeerNodeMetadata(MetadataSharedPtr metadata);

  // traffic direction, inbound or outbound
  envoy::config::core::v3::TrafficDirection traffic_direction_;

  google::protobuf::Struct peer_node_metadata_;
  IstioStats& istio_stats_;
  const std::string& destination_service_;
};

} // namespace IstioStats
} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
