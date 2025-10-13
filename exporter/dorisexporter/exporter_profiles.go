package dorisexporter

import (
	"context"
	_ "embed" // for SQL file embedding
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.uber.org/zap"
)

//go:embed sql/profiles_sample_ddl.sql
var profilesSampleDDL string

//go:embed sql/profiles_stack_ddl.sql
var profilesStackDDL string

//go:embed sql/profiles_stack_job.sql
var profilesStackJob string

type dProfileSample struct {
	ServiceName string         `json:"service_name"`
	Timestamp   string         `json:"timestamp"`
	Attributes  map[string]any `json:"attributes"`
	ProfileType string         `json:"profile_type"`
	SampleType  string         `json:"sample_type"`
	SampleUnit  string         `json:"sample_unit"`
	PeriodType  string         `json:"period_type"`
	PeriodUnit  string         `json:"period_unit"`
	Period      int64          `json:"period"`
	SampleValue int64          `json:"sample_value"`
	StackHash   uint64         `json:"stack_hash"`
}

type dProfileStack struct {
	ServiceName string   `json:"service_name"`
	StackHash   uint64   `json:"stack_hash"`
	LastSeen    string   `json:"last_seen"`
	Stack       []string `json:"stack"`
}

type profilesExporter struct {
	*commonExporter
}

func newProfilesExporter(logger *zap.Logger, cfg *Config, set component.TelemetrySettings) *profilesExporter {
	return &profilesExporter{
		commonExporter: newExporter(logger, cfg, set, "PROFILE"),
	}
}

func (e *profilesExporter) start(ctx context.Context, host component.Host) error {
	client, err := createDorisHTTPClient(ctx, e.cfg, host, e.TelemetrySettings)
	if err != nil {
		return err
	}
	e.client = client

	if e.cfg.CreateSchema {
		conn, err := createDorisMySQLClient(e.cfg)
		if err != nil {
			return err
		}
		defer conn.Close()

		err = createAndUseDatabase(ctx, conn, e.cfg)
		if err != nil {
			return err
		}

		for _, ddl := range []string{
			fmt.Sprintf(profilesSampleDDL, e.cfg.Profiles, e.cfg.propertiesStr()),
			fmt.Sprintf(profilesStackDDL, e.cfg.Profiles, e.cfg.propertiesStrForNoDynamicPartition())} {
			_, err = conn.ExecContext(ctx, ddl)
			if err != nil {
				return err
			}
		}

		// TODO doris does not support job for data deletion
		// dropJob := e.formatDropProfilesStackJob()
		// _, err = conn.ExecContext(ctx, dropJob)
		// if err != nil {
		// 	e.logger.Warn("failed to drop job", zap.Error(err))
		// }

		// if e.cfg.HistoryDays > 0 {
		// 	job := e.formatProfilesStackJob()
		// 	_, err = conn.ExecContext(ctx, job)
		// 	if err != nil {
		// 		e.logger.Warn("failed to create job", zap.Error(err))
		// 	}
		// }

	}

	go e.reporter.report()
	return nil
}

func (e *profilesExporter) formatDropProfilesStackJob() string {
	return fmt.Sprintf(
		"DROP JOB where jobName = '%s:%s_stack_clean_expired_data_daily';",
		e.cfg.Database,
		e.cfg.Traces,
	)
}

func (e *profilesExporter) formatProfilesStackJob() string {
	nowUTC := time.Now().UTC()
	beginTime := time.Date(
		nowUTC.Year(),
		nowUTC.Month(),
		nowUTC.Day(),
		1, 0, 0, 0,
		nowUTC.Location(),
	)
	return fmt.Sprintf(
		profilesStackJob,
		e.cfg.Database,
		e.cfg.Profiles,
		beginTime.Format(time.DateTime),
		e.cfg.Profiles,
		e.cfg.HistoryDays,
	)
}

func (e *profilesExporter) shutdown(_ context.Context) error {
	if e.client != nil {
		e.client.CloseIdleConnections()
	}
	return nil
}

func (e *profilesExporter) pushProfilesData(ctx context.Context, pd pprofile.Profiles) error {
	var (
		dictionary     = pd.Dictionary()
		stringTable    = dictionary.StringTable()
		attributeTable = dictionary.AttributeTable()
		stackTable     = dictionary.StackTable()
		locationTable  = dictionary.LocationTable()
		functionTable  = dictionary.FunctionTable()
	)

	getString := func(index int32) string {
		i := int(index)
		if i < 0 || i >= stringTable.Len() {
			return ""
		}
		return stringTable.At(i)
	}

	lastSeen := e.formatTime(time.Now())
	var (
		samples []*dProfileSample
		stacks  []*dProfileStack
	)
	for _, resourceProfiles := range pd.ResourceProfiles().All() {
		serviceName := ""
		resourceAttributes := map[string]any{}
		for key, val := range resourceProfiles.Resource().Attributes().All() {
			if key == string(semconv.ServiceNameKey) {
				serviceName = val.AsString()
			}
			if toKey, ok := attributeKeyConvertMap[key]; ok {
				resourceAttributes[toKey] = val.AsRaw()
			}
		}

		for _, scopeProfiles := range resourceProfiles.ScopeProfiles().All() {
			for _, profile := range scopeProfiles.Profiles().All() {
				var (
					sampleType = getString(profile.SampleType().TypeStrindex())
					sampleUnit = getString(profile.SampleType().UnitStrindex())
					periodType = getString(profile.PeriodType().TypeStrindex())
					periodUnit = getString(profile.PeriodType().UnitStrindex())
					period     = profile.Period()
				)
				// TODO to be distinguished
				profileType := strings.Join([]string{"ebpf", sampleType, sampleUnit, periodType, periodUnit}, ":")

				for _, sample := range profile.Sample().All() {
					stack := stackTable.At(int(sample.StackIndex()))
					var stackStrings []string
					for _, locationIndex := range stack.LocationIndices().All() {
						if locationIndex := int(locationIndex); locationIndex < locationTable.Len() {
							var lineStrings []string
							location := locationTable.At(locationIndex)
							for _, line := range location.Line().All() {
								if functionIndex := int(line.FunctionIndex()); functionIndex < functionTable.Len() {
									function := functionTable.At(functionIndex)
									name, filename := getString(function.NameStrindex()), getString(function.FilenameStrindex())
									if name != "" {
										lineStrings = append(lineStrings, fmt.Sprintf("%s %s:%d", name, filename, function.StartLine()))
									}
								}
							}
							if len(lineStrings) == 0 {
								stackStrings = append(stackStrings, fmt.Sprintf("%#x", location.Address()))
							} else {
								stackStrings = append(stackStrings, lineStrings...)
							}
						}
					}

					var profileStack *dProfileStack
					if len(stackStrings) > 0 {
						stackHash := StackHash(stackStrings)
						profileStack = &dProfileStack{
							ServiceName: serviceName,
							StackHash:   stackHash,
							LastSeen:    lastSeen,
							Stack:       stackStrings,
						}
						stacks = append(stacks, profileStack)
					}

					attributes := map[string]any{}
					for _, attributeindex := range sample.AttributeIndices().All() {
						if attributeindex := int(attributeindex); attributeindex < attributeTable.Len() {
							attribute := attributeTable.At(attributeindex)
							if keyString := getString(attribute.KeyStrindex()); keyString != "" {
								if toKey, ok := attributeKeyConvertMap[keyString]; ok {
									attributes[toKey] = attribute.Value().AsRaw()
								}
							}
						}
					}
					for k, v := range resourceAttributes {
						attributes[k] = v
					}

					for i, timestampsUnixNano := range sample.TimestampsUnixNano().All() {
						var sampleValue int64 = 1
						if i < sample.Values().Len() {
							sampleValue = sample.Values().At(i)
						}

						sample := &dProfileSample{
							ServiceName: serviceName,
							Timestamp:   e.formatTime(pcommon.Timestamp(timestampsUnixNano).AsTime()),
							Attributes:  attributes,
							ProfileType: profileType,
							SampleType:  sampleType,
							SampleUnit:  sampleUnit,
							PeriodType:  periodType,
							PeriodUnit:  periodUnit,
							Period:      period,
							SampleValue: sampleValue,
						}
						if profileStack != nil {
							sample.StackHash = profileStack.StackHash
						}
						samples = append(samples, sample)
					}
				}
			}
		}
	}

	if err := e.pushProfilesStack(ctx, stacks); err != nil {
		return err
	}
	if err := e.pushProfilesSample(ctx, samples); err != nil {
		return err
	}
	return nil
}

func (e *profilesExporter) pushProfilesStack(ctx context.Context, stacks []*dProfileStack) error {
	if len(stacks) == 0 {
		return nil
	}
	marshal, err := toJSONLines(stacks)
	if err != nil {
		return err
	}
	table := e.cfg.Profiles + "_stack"
	label := generateLabel(e.cfg, table)
	req, err := streamLoadRequest(ctx, e.cfg, table, marshal, label)
	if err != nil {
		return err
	}

	res, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	response := streamLoadResponse{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}

	if response.success() {
		e.reporter.incrTotalRows(int64(len(stacks)))
		e.reporter.incrTotalBytes(int64(len(marshal)))

		if response.duplication() {
			e.logger.Warn("label already exists", zap.String("label", label), zap.Int("skipped", len(stacks)))
		}

		if e.cfg.LogResponse {
			e.logger.Info("profile response:\n" + string(body))
		} else {
			e.logger.Debug("profile response:\n" + string(body))
		}
		return nil
	}

	return fmt.Errorf("failed to push profile data, response:%s", string(body))
}

func (e *profilesExporter) pushProfilesSample(ctx context.Context, samples []*dProfileSample) error {
	if len(samples) == 0 {
		return nil
	}
	marshal, err := toJSONLines(samples)
	if err != nil {
		return err
	}
	table := e.cfg.Profiles + "_sample"
	label := generateLabel(e.cfg, table)
	req, err := streamLoadRequest(ctx, e.cfg, table, marshal, label)
	if err != nil {
		return err
	}

	res, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	response := streamLoadResponse{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}

	if response.success() {
		e.reporter.incrTotalRows(int64(len(samples)))
		e.reporter.incrTotalBytes(int64(len(marshal)))

		if response.duplication() {
			e.logger.Warn("label already exists", zap.String("label", label), zap.Int("skipped", len(samples)))
		}

		if e.cfg.LogResponse {
			e.logger.Info("profile response:\n" + string(body))
		} else {
			e.logger.Debug("profile response:\n" + string(body))
		}
		return nil
	}

	return fmt.Errorf("failed to push profile data, response:%s", string(body))
}

func int64ToBytes(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

func StackHash(s []string) uint64 {
	h := fnv.New64a()
	for _, l := range s {
		_, _ = h.Write([]byte(l))
	}
	return h.Sum64()
}

var attributeKeyConvertMap = map[string]string{
	string(semconv.ProcessExecutableNameKey): "comm",
	string(semconv.K8SContainerNameKey):      "container",
	string(semconv.K8SPodNameKey):            "pod",
	string(semconv.K8SNamespaceNameKey):      "namespace",
	string(semconv.K8SNodeNameKey):           "node",
	string(semconv.K8SClusterNameKey):        "cluster",
}
