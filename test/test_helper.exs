ExUnit.start(capture_log: true)

provider_workers =
  if System.get_env("PIGEON_SKIP_PROVIDER_WORKERS") == "true" do
    []
  else
    [
      PigeonTest.ADM,
      PigeonTest.APNS,
      PigeonTest.APNS.JWT,
      PigeonTest.FCM,
      PigeonTest.LegacyFCM
    ]
  end

workers = provider_workers ++ [PigeonTest.Sandbox]

Supervisor.start_link(workers, strategy: :one_for_one)
