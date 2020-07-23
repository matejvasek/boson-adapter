package cmd

import (
	"boson-adapter/boson"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "boson-adapter --registrationUri=<URI> --applicationId <NAME> --applicationVersion <VERSION> --skin <URI> --concurrency <NUMBER>",
	Short: "ProjectB polling adapter. Polls event from ProjectB host and sends them to the sink.",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		var regURL, appID, appVer, sinkURL, mapping string
		stringArgs := map[string]*string{
			"registrationUri":    &regURL,
			"applicationId":      &appID,
			"applicationVersion": &appVer,
			"sink":               &sinkURL,
			"mapping":            &mapping,
		}
		for name, ptr := range stringArgs {
			*ptr, err = cmd.Flags().GetString(name)
			if err != nil {
				return err
			}
		}
		concurrency, err := cmd.Flags().GetInt("concurrency")
		if err != nil {
			return err
		}

		var m boson.ProjectBKnativeEventMapping
		err = json.Unmarshal([]byte(mapping), &m)
		if err != nil {
			return err
		}

		config := boson.AdapterConfig{
			ApplicationID:      appID,
			ApplicationVersion: appVer,
			Concurrency:        concurrency,
			ProjectBHostURL:    regURL,
			BosonFunctionURL:   sinkURL,
			Mapping:            []boson.ProjectBKnativeEventMapping{m},
			Logger:             log.New(os.Stderr, "", log.LstdFlags),
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		adapter, err := boson.NewAdapter(config)
		if err != nil {
			return err
		}

		stopChan := make(chan struct{}, 1)
		go func() {
			<-sigs
			stopChan <- struct{}{}
		}()

		err = adapter.Start(stopChan)
		if err != nil {
			return fmt.Errorf("cannot start worker: %v", err.Error())
		}

		return nil
	},
}

func init() {
	rootCmd.Flags().String("registrationUri", "http://localhost:5001", "Uri of ProjectB Host")
	rootCmd.Flags().String("applicationId", "knative-adapter", "application identifier")
	rootCmd.Flags().String("applicationVersion", "1.0", "application version")
	rootCmd.Flags().String("sink", "http://localhost:8080", "sink where to push the events")
	rootCmd.Flags().Int("concurrency", runtime.NumCPU(), "number of possible concurrent events being processed")
	rootCmd.Flags().String("mapping", "", "mapping between projectb event and knative event (JSON)")
}
