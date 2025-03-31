#include "src/meta_protocol_proxy/filters/istio_stats/istio_stats.h"

#include <memory>
#include <string>
#include <vector>

#include "envoy/stats/scope.h"

#include "source/common/stats/symbol_table.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace IstioStats {

IstioStats::IstioStats(Server::Configuration::FactoryContext& context,
                       envoy::config::core::v3::TrafficDirection traffic_direction)
    : scope_(context.scope()), pool_(context.scope().symbolTable()),
      stat_namespace_(pool_.add(CustomStatNamespace)),
      requests_total_(pool_.add("istio_requests_total")),
      request_duration_milliseconds_(pool_.add("istio_request_duration_milliseconds")),
      request_bytes_(pool_.add("istio_request_bytes")),
      response_bytes_(pool_.add("istio_response_bytes")), empty_(pool_.add("")),
      unknown_(pool_.add("unknown")), source_(pool_.add("source")),
      destination_(pool_.add("destination")), latest_(pool_.add("latest")),
      http_(pool_.add("http")), grpc_(pool_.add("grpc")), tcp_(pool_.add("tcp")),
      mutual_tls_(pool_.add("mutual_tls")), none_(pool_.add("none")),
      reporter_(pool_.add("reporter")), source_workload_(pool_.add("source_workload")),
      source_workload_namespace_(pool_.add("source_workload_namespace")),
      source_principal_(pool_.add("source_principal")), source_app_(pool_.add("source_app")),
      source_version_(pool_.add("source_version")),
      source_canonical_service_(pool_.add("source_canonical_service")),
      source_canonical_revision_(pool_.add("source_canonical_revision")),
      source_cluster_(pool_.add("source_cluster")),
      destination_workload_(pool_.add("destination_workload")),
      destination_workload_namespace_(pool_.add("destination_workload_namespace")),
      destination_principal_(pool_.add("destination_principal")),
      destination_app_(pool_.add("destination_app")),
      destination_version_(pool_.add("destination_version")),
      destination_service_(pool_.add("destination_service")),
      destination_service_name_(pool_.add("destination_service_name")),
      destination_service_namespace_(pool_.add("destination_service_namespace")),
      destination_canonical_service_(pool_.add("destination_canonical_service")),
      destination_canonical_revision_(pool_.add("destination_canonical_revision")),
      destination_cluster_(pool_.add("destination_cluster")),
      request_protocol_(pool_.add("request_protocol")),
      response_flags_(pool_.add("response_flags")),
      connection_security_policy_(pool_.add("connection_security_policy")),
      response_code_(pool_.add("response_code")) {
  traffic_direction_ = traffic_direction;
  if (context.serverFactoryContext().localInfo().node().has_metadata()) {
    local_node_metadata_.CopyFrom(context.serverFactoryContext().localInfo().node().metadata());
  } else {
    local_node_metadata_.Clear();
  }
}

// Return String from  Struct
static std::string getStringFromStruct(const google::protobuf::Struct& s, const std::string& key) {
  auto it = s.fields().find(key);
  return (it != s.fields().end()) ? it->second.string_value() : "";
}

// Return SubStruct
static const google::protobuf::Struct& getSubStruct(const google::protobuf::Struct& s, const std::string& key) {
  static const google::protobuf::Struct empty_struct;
  auto it = s.fields().find(key);
  return (it != s.fields().end()) ? it->second.struct_value() : empty_struct;
}

void IstioStats::report(const google::protobuf::Struct& peer_metadata, MetadataSharedPtr metadata,
                        const std::string& destination_service) {
  Stats::StatNameTagVector tags;
  tags.reserve(25);

  if (traffic_direction_ == envoy::config::core::v3::TrafficDirection::INBOUND) {
    tags.push_back({reporter_, destination_});
    populateSourceTagsFromStruct(peer_metadata, tags);
    populateDestTagsFromStruct(local_node_metadata_, tags);
    // use the destination_service in the stats config for inbound traffic
    auto destination_service_name = pool_.add(destination_service);
    tags.push_back({destination_service_, destination_service_name});
    tags.push_back({destination_service_name_, destination_service_name});
  } else {
    tags.push_back({reporter_, source_});
    populateSourceTagsFromStruct(local_node_metadata_, tags);
    populateDestTagsFromStruct(peer_metadata, tags);
    // extract the destination_service from the cluster name for outbound traffic
    if (metadata->streamInfo().upstreamClusterInfo().has_value() &&
        metadata->streamInfo().upstreamClusterInfo().value()) {
      std::string cluster_name = metadata->streamInfo().upstreamClusterInfo().value()->name();
      size_t pos = cluster_name.find_last_of("|");
      if (pos != std::string::npos) {
        cluster_name = cluster_name.substr(pos + 1, cluster_name.length() - pos - 1);
      }
      auto destination_service_name = pool_.add(cluster_name);
      tags.push_back({destination_service_, destination_service_name});
      tags.push_back({destination_service_name_, destination_service_name});
    }
  }
  tags.push_back(
      {response_code_, pool_.add(absl::StrCat(static_cast<int>(metadata->getResponseStatus())))});
  Stats::Utility::counterFromStatNames(scope_, {stat_namespace_, requests_total_}, tags).inc();
  auto duration = metadata->streamInfo().requestComplete();
  if (duration.has_value()) {
    Stats::Utility::histogramFromStatNames(scope_,
                                           {stat_namespace_, request_duration_milliseconds_},
                                           Stats::Histogram::Unit::Milliseconds, tags)
        .recordValue(absl::FromChrono(duration.value()) / absl::Milliseconds(1));
  }
  Stats::Utility::histogramFromStatNames(scope_, {stat_namespace_, request_bytes_},
                                         Stats::Histogram::Unit::Bytes, tags)
      .recordValue(metadata->streamInfo().bytesSent());
  Stats::Utility::histogramFromStatNames(scope_, {stat_namespace_, response_bytes_},
                                         Stats::Histogram::Unit::Bytes, tags)
      .recordValue(metadata->streamInfo().bytesReceived());
}

void IstioStats::populateSourceTagsFromStruct(const google::protobuf::Struct& metadata,
                                        Stats::StatNameTagVector& tags) {
  auto workload = getStringFromStruct(metadata, "WORKLOAD_NAME");
  tags.push_back({source_workload_, !workload.empty() ? pool_.add(std::string_view(workload.data(), workload.size())) : unknown_});
  auto ns = getStringFromStruct(metadata, "NAMESPACE");
  tags.push_back({source_workload_namespace_, !ns.empty() ? pool_.add(std::string_view(ns.data(), ns.size())) : unknown_});
  auto cluster = getStringFromStruct(metadata, "CLUSTER_ID");
  tags.push_back({source_cluster_, !cluster.empty() ? pool_.add(std::string_view(cluster.data(), cluster.size())) : unknown_});
  const auto& labels = getSubStruct(metadata, "LABELS");
  if (!labels.fields().empty()) {
    auto app = getStringFromStruct(labels, "app");
    auto app_view = (!app.empty() ? std::string_view(app.data(), app.size()) : std::string_view());
    tags.push_back({source_app_, !app_view.empty() ? pool_.add(app_view) : unknown_});

    auto version = getStringFromStruct(labels, "version");
    auto version_view = (!version.empty() ? std::string_view(version.data(), version.size()) : std::string_view());
    tags.push_back({source_version_, !version_view.empty() ? pool_.add(version_view) : unknown_});

    auto name = getStringFromStruct(labels, "service.istio.io/canonical-name");
    std::string_view name_view;
    if (name.empty()) {
      name_view = (!workload.empty() ? std::string_view(workload.data(), workload.size()) : std::string_view());
    } else {
      name_view = std::string_view(name.data(), name.size());
    }
    tags.push_back(
        {source_canonical_service_, !name_view.empty() ? pool_.add(name_view) : unknown_});

    auto rev = getStringFromStruct(labels, "service.istio.io/canonical-revision");
    if (!rev.empty()) {
      auto rev_view = std::string_view(rev.data(), rev.size());
      tags.push_back(
          {source_canonical_revision_, !rev_view.empty() ? pool_.add(rev_view) : unknown_});
    } else {
      tags.push_back({source_canonical_revision_, latest_});
    }
  } else {
    tags.push_back({source_app_, unknown_});
    tags.push_back({source_version_, unknown_});
    tags.push_back({source_canonical_service_, unknown_});
    tags.push_back({source_canonical_revision_, latest_});
  }
}

void IstioStats::populateDestTagsFromStruct(const google::protobuf::Struct& metadata,
                                             Stats::StatNameTagVector& tags) {
  auto workload = getStringFromStruct(metadata, "WORKLOAD_NAME");
  tags.push_back({destination_workload_, !workload.empty() ? pool_.add(std::string_view(workload.data(), workload.size())) : unknown_});
  auto ns = getStringFromStruct(metadata, "NAMESPACE");
  tags.push_back({destination_service_namespace_, !ns.empty() ? pool_.add(std::string_view(ns.data(), ns.size())) : unknown_});
  tags.push_back({destination_workload_namespace_, !ns.empty() ? pool_.add(std::string_view(ns.data(), ns.size())) : unknown_});
  auto cluster = getStringFromStruct(metadata, "CLUSTER_ID");
  tags.push_back({destination_cluster_, !cluster.empty() ? pool_.add(std::string_view(cluster.data(), cluster.size())) : unknown_});
  const auto& labels = getSubStruct(metadata, "LABELS");
  if (!labels.fields().empty()) {
    auto app = getStringFromStruct(labels, "app");
    auto app_view = (!app.empty() ? std::string_view(app.data(), app.size()) : std::string_view());
    tags.push_back({destination_app_, !app_view.empty() ? pool_.add(app_view) : unknown_});

    auto version = getStringFromStruct(labels, "version");
    auto version_view = (!version.empty() ? std::string_view(version.data(), version.size()) : std::string_view());
    tags.push_back(
        {destination_version_, !version_view.empty() ? pool_.add(version_view) : unknown_});

    auto name = getStringFromStruct(labels, "service.istio.io/canonical-name");
    std::string_view name_view;
    if (name.empty()) {
      name_view = (!workload.empty() ? std::string_view(workload.data(), workload.size()) : std::string_view());
    } else {
      name_view = std::string_view(name.data(), name.size());
    }
    tags.push_back(
        {destination_canonical_service_, !name_view.empty() ? pool_.add(name_view) : unknown_});

    auto rev = getStringFromStruct(labels, "service.istio.io/canonical-revision");
    if (!rev.empty()) {
      auto rev_view = std::string_view(rev.data(), rev.size());
      tags.push_back(
          {destination_canonical_revision_, !rev_view.empty() ? pool_.add(rev_view) : unknown_});
    } else {
      tags.push_back({destination_canonical_revision_, latest_});
    }
  } else {
    tags.push_back({destination_app_, unknown_});
    tags.push_back({destination_version_, unknown_});
    tags.push_back({destination_canonical_service_, unknown_});
    tags.push_back({destination_canonical_revision_, latest_});
  }
}

} // namespace IstioStats
} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
