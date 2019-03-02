void NearSyncTest::LoadConfigurationDone(
  Configuration::Ptr config,
  Configuration::Ptr *out_config,
  Notification *notification) const {

  CHECK(config);
  *out_config = config;
  notification->Notify();
}

Test() {
  LOG(INFO) << "Testing correct check for mixed hyperviser";
  // First step is to modify the zeus config to have
  // parent management server list as the first entry in the
  // management_server_list

  // Load Zeus configuration from Zookeeper.
  Zeus::Ptr zeus = make_shared<Zeus>();
  Notification notification;
  Configuration::Ptr config;
  Function<void(Configuration::Ptr)> load_done_cb =
    bind(&LoadConfigurationDone, this, _1, &config, &notification);
  zeus->LoadConfiguration(load_done_cb);
  notification.Wait();
  CHECK(config);

  // Update Zeus configuration.
  config_proto = *config->config_proto()
  shared_ptr<ConfigurationProto> new_config_proto =
    make_shared<ConfigurationProto>();
  new_config_proto->CopyFrom(config_proto);
  new_config_proto->clear_management_server_list()
  // copy parent management server as the first entry
  for (int xx = 0; xx < config_proto->management_server_list_size(); ++xx) {
    const ConfigurationProto::ManagementServer& ms_entry =
      config_proto->management_server_list(xx);
    if (!ms_entry.has_management_server_type()) {
      ConfigurationProto::ManagementServer pms =
        new_config_proto->add_management_server_list()
      pms->CopyFrom(ms_entry);
      break;
    }
  }
  // and then copy the rest of management server entries
  for (int xx = 0; xx < config_proto->management_server_list_size(); ++xx) {
    const ConfigurationProto::ManagementServer& ms_entry =
      config_proto->management_server_list(xx);
    if (ms_entry.has_management_server_type()) {
      ConfigurationProto::ManagementServer pms =
        new_config_proto->add_management_server_list()
      pms->CopyFrom(ms_entry);
    }
  }

  new_config_proto->set_logical_timestamp(
    new_config_proto->logical_timestamp() + 1);
  Configuration::Ptr new_config = make_shared<Configuration>(new_config_proto);
  notification.Reset();
  Function<void(bool)> update_done_cb =
    bind(&BlockStoreTester::UpdateConfigurationDone,
         this, _1, &notification);
  zeus->UpdateConfiguration(new_config, update_done_cb);
  notification.Wait();
  LOG(INFO) << "The zeus config is updated";

  // On local site, set exclusive gflags to satisfy all criteria for enabling
  // near sync as near_sync_test_hook_override_pre_checks_for_qa_test will
  // bypass the check for Mixed Hypervisor
  local_site()->SetCerebroGFlag(
    "near_sync_test_hook_override_pre_checks_for_qa_test", "false");
  remote_site->SetCerebroGFlag(
    "near_sync_test_hook_override_pre_checks_for_qa_test", "true");

  local_site()->SetCerebroGFlag("cerebro_near_sync_cluster_min_size", "1");
  remote_site->SetCerebroGFlag("cerebro_near_sync_cluster_min_size", "1");
  const int old_val = node_min_ssd_capacity_mb_;
  local_site()->SetCerebroGFlag(
    "near_sync_min_ssd_capacity_disk_size_mb", "10240"); // 10 GB

  sleep(2);
  VerifyCerebroFeature(CerebroCapabilities::kNearSyncDr, enabled);
  VerifyCerebroFeature(CerebroCapabilities::kNearSyncDr, enabled, remote_site);
  LOG(INFO) << "Test Succeeded";
