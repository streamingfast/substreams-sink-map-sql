package main

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"substreams-sink-map-sql/data"
	sql2 "substreams-sink-map-sql/sql"
	"time"

	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/jhump/protoreflect/desc"
	"github.com/spf13/cobra"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/client"
	"go.uber.org/zap"
)

type PsqlInfo struct {
	Host     string
	Port     int
	User     string
	Password string
	Dbname   string
}

func (i *PsqlInfo) GetPsqlInfo() string {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		i.Host, i.Port, i.User, i.Password, i.Dbname,
	)
	return psqlInfo
}

var RootCmd = &cobra.Command{
	Use:   "sql-sync <endpoint> <manifest> <module>",
	Short: "Sql sink data from a substreams output to a sql database",
	RunE:  rootRun,
	Args:  cobra.ExactArgs(3),
}

func init() {
	RootCmd.Flags().Bool("insecure", false, "Skip TLS certificate verification")
	RootCmd.Flags().Bool("plaintext", false, "Use plaintext connection")

	// Database
	RootCmd.Flags().String("db-host", "localhost", "PostgreSQL host endpoint")
	RootCmd.Flags().Int("db-port", 5432, "PostgreSQL port")
	RootCmd.Flags().String("db-user", "postgres", "PostgreSQL user")
	RootCmd.Flags().String("db-name", "postgres", "PostgreSQL database name")
	RootCmd.Flags().Uint64("start-block", 0, "start block number (0 means no start block)")
	RootCmd.Flags().Uint64("stop-block", 0, "stop block number (0 means no stop block)")
	RootCmd.Flags().Duration("startup-delay", time.Duration(0), "stop block number (0 means no stop block)")
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		panic(err)
	}
	fmt.Println("Goodbye!")
}

func rootRun(cmd *cobra.Command, args []string) error {
	apiToken := os.Getenv("SUBSTREAMS_API_TOKEN")
	if apiToken == "" {
		return fmt.Errorf("missing SUBSTREAMS_API_TOKEN environment variable")
	}

	if sflags.MustGetDuration(cmd, "startup-delay") != time.Duration(0) {
		time.Sleep(sflags.MustGetDuration(cmd, "startup-delay"))
	}

	logger, tracer := logging.ApplicationLogger("honey-tracker", "honey-tracker")

	endpoint := args[0]
	manifestPath := args[1]
	outputModuleName := args[2]

	flagInsecure := sflags.MustGetBool(cmd, "insecure")
	flagPlaintext := sflags.MustGetBool(cmd, "plaintext")
	startBlock := sflags.MustGetUint64(cmd, "start-block")
	stopBlock := sflags.MustGetUint64(cmd, "start-block")

	clientConfig := client.NewSubstreamsClientConfig(
		endpoint,
		apiToken,
		client.JWT,
		flagInsecure,
		flagPlaintext,
	)

	spkg, module, outputModuleHash, br, err := sink.ReadManifestAndModuleAndBlockRange(
		manifestPath,
		"",
		nil,
		outputModuleName,
		"hivemapper.types.v1.Transactions",
		false,
		"",
		logger,
	)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	options := []sink.Option{
		sink.WithBlockRange(br),
	}

	if startBlock > 0 && stopBlock > 0 {
		blockRange, err := bstream.NewRangeContaining(startBlock, stopBlock)
		if err != nil {
			return fmt.Errorf("creating block range: %w", err)
		}
		options = append(options, sink.WithBlockRange(blockRange))
	}

	s, err := sink.New(
		sink.SubstreamsModeProduction,
		false,
		spkg,
		module,
		outputModuleHash,
		clientConfig,
		logger,
		tracer,
		options...,
	)
	if err != nil {
		return fmt.Errorf("creating sink: %w", err)
	}

	deps := map[string]*desc.FileDescriptor{}
	err = resolveDependencies(spkg.ProtoFiles, "hivemapper/v1/hivemapper.proto", deps)
	if err != nil {
		return fmt.Errorf("resolving dependencies: %w", err)
	}

	var fd *desc.FileDescriptor
	for _, p := range spkg.ProtoFiles {
		if *p.Name != "hivemapper/v1/hivemapper.proto" {
			continue
		}

		fd, err = desc.CreateFileDescriptor(p, slices.Collect(maps.Values(deps))...)
		if err != nil {
			return fmt.Errorf("creating file descriptor: %w", err)
		}
		break
	}

	if fd == nil {
		return fmt.Errorf("could not find file descriptor")
	}

	psqlInfo := &PsqlInfo{
		Host:     sflags.MustGetString(cmd, "db-host"),
		Port:     sflags.MustGetInt(cmd, "db-port"),
		User:     sflags.MustGetString(cmd, "db-user"),
		Password: os.Getenv("POSTGRES_DB_PWD"),
		Dbname:   sflags.MustGetString(cmd, "db-name"),
	}

	db, err := sql.Open("postgres", psqlInfo.GetPsqlInfo())
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	outputType := ""
	for _, m := range spkg.Modules.Modules {
		if m.Name == outputModuleName {
			outputType = strings.TrimPrefix(m.Output.Type, "proto:")
			break
		}
	}

	if outputType == "" {
		return fmt.Errorf("could not find output type for module %s", outputModuleName)
	}

	schema, err := sql2.NewSchema("myschema", 1, outputType, fd, logger)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	database, err := sql2.NewDatabase(schema, db, outputType, fd, logger)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	ctx := context.Background()
	sinker := data.NewSinker(logger, s, database)
	sinker.OnTerminating(func(err error) {
		logger.Error("sinker terminating", zap.Error(err))
	})

	err = sinker.Run(ctx)
	if err != nil {
		return fmt.Errorf("runnning sinker:%w", err)
	}

	return nil
}

func resolveDependencies(fds []*descriptorpb.FileDescriptorProto, fileName string, deps map[string]*desc.FileDescriptor) error {
	if deps[fileName] != nil {
		return nil
	}

	for _, fd := range fds {
		if fileName == fd.GetName() {
			if len(fd.Dependency) != 0 {
				for _, dep := range fd.Dependency {
					err := resolveDependencies(fds, dep, deps)
					if err != nil {
						return err
					}
				}
			}

			d, err := desc.CreateFileDescriptor(fd, slices.Collect(maps.Values(deps))...)
			if err != nil {
				return fmt.Errorf("creating file descriptor: %w", err)
			}
			deps[fd.GetName()] = d
		}
	}
	return nil
}
