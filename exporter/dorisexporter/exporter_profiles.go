package dorisexporter

import (
	"context"
	_ "embed" // for SQL file embedding
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cespare/xxhash"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/ebpf-profiler/libpf"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.uber.org/zap"
)

//go:embed sql/profiles_ddl.sql
var profilesDDL string

type dProfile struct {
	ServiceName        string         `json:"service_name"`
	Timestamp          string         `json:"timestamp"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	ScopeName          string         `json:"scope_name"`
	ScopeVersion       string         `json:"scope_version"`
	SampleType         string         `json:"sample_type"`
	SampleUnit         string         `json:"sample_unit"`
	PeriodType         string         `json:"period_type"`
	PeriodUnit         string         `json:"period_unit"`
	SampleValue        int64          `json:"sample_value"`
	Attributes         map[string]any `json:"attributes"`
	Locations          []*dLocation   `json:"locations"`
	StackTraceID       string         `json:"stack_trace_id"`
}

type dLocation struct {
	Level int64  `json:"level"`
	Line  string `json:"line"`
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

		ddl := fmt.Sprintf(profilesDDL, e.cfg.Profiles, e.cfg.propertiesStr())
		_, err = conn.ExecContext(ctx, ddl)
		if err != nil {
			return err
		}
	}

	go e.reporter.report()
	return nil
}

func (e *profilesExporter) shutdown(_ context.Context) error {
	if e.client != nil {
		e.client.CloseIdleConnections()
	}
	return nil
}

func (e *profilesExporter) pushProfilesData(ctx context.Context, pd pprofile.Profiles) error {
	label := generateLabel(e.cfg, e.cfg.Profiles)
	var profiles []*dProfile
	dictionary := pd.Dictionary()
	stringTable := dictionary.StringTable()
	attributeTable := dictionary.AttributeTable()
	stackTable := dictionary.StackTable()
	locationTable := dictionary.LocationTable()
	functionTable := dictionary.FunctionTable()
	for _, resourceProfiles := range pd.ResourceProfiles().All() {
		serviceName := ""
		v, ok := resourceProfiles.Resource().Attributes().Get(string(semconv.ServiceNameKey))
		if ok {
			serviceName = v.AsString()
		}
		for _, scopeProfiles := range resourceProfiles.ScopeProfiles().All() {
			for _, profile := range scopeProfiles.Profiles().All() {
				sampleType := stringTable.At(int(profile.SampleType().TypeStrindex()))
				sampleUnit := stringTable.At(int(profile.SampleType().UnitStrindex()))
				periodType := stringTable.At(int(profile.PeriodType().TypeStrindex()))
				periodUnit := stringTable.At(int(profile.PeriodType().UnitStrindex()))
				for _, sample := range profile.Sample().All() {
					attributes := make(map[string]any)
					for _, attributeindex := range sample.AttributeIndices().All() {
						if attributeindex > int32(attributeTable.Len()) {
							continue
						}
						attribute := attributeTable.At(int(attributeindex))
						keyString := stringTable.At(int(attribute.KeyStrindex()))
						attributes[keyString] = attribute.Value().AsRaw()
					}
					stack := stackTable.At(int(sample.StackIndex()))
					var dLocations []*dLocation
					for _, locIndex := range stack.LocationIndices().All() {
						location := locationTable.At(int(locIndex))
						for _, line := range location.Line().All() {
							function := functionTable.At(int(line.FunctionIndex()))
							if function.NameStrindex() > 0 {
								dLocations = append(dLocations, &dLocation{
									Line: stringTable.At(int(function.NameStrindex())),
								})
							} else if function.SystemNameStrindex() > 0 {
								dLocations = append(dLocations, &dLocation{
									Line: stringTable.At(int(function.SystemNameStrindex())),
								})
							}
						}
					}
					if l := len(dLocations); l > 0 {
						hasher := xxhash.New()
						for _, dLocation := range dLocations {
							dLocation.Level = int64(l)
							l--
							_, _ = hasher.Write([]byte(dLocation.Line))
							_, _ = hasher.Write(int64ToBytes(dLocation.Level))
						}
						h := hasher.Sum64()
						fileID := libpf.NewFileID(h, h)

						for i, timestampsUnixNano := range sample.TimestampsUnixNano().All() {
							var sampleValue int64 = 1
							if i < sample.Values().Len() {
								sampleValue = sample.Values().At(i)
							}

							profiles = append(profiles, &dProfile{
								ServiceName:        serviceName,
								Timestamp:          e.formatTime(pcommon.Timestamp(timestampsUnixNano).AsTime()),
								ResourceAttributes: resourceProfiles.Resource().Attributes().AsRaw(),
								ScopeName:          scopeProfiles.Scope().Name(),
								ScopeVersion:       scopeProfiles.Scope().Version(),
								SampleType:         sampleType,
								SampleUnit:         sampleUnit,
								PeriodType:         periodType,
								PeriodUnit:         periodUnit,
								SampleValue:        sampleValue,
								Attributes:         attributes,
								Locations:          dLocations,
								StackTraceID:       fileID.Base64(),
							})
						}
					}
				}
			}
		}
	}
	return e.pushProfileDataInternal(ctx, profiles, label)
}

func (e *profilesExporter) pushProfileDataInternal(ctx context.Context, profiles []*dProfile, label string) error {
	marshal, err := toJSONLines(profiles)
	if err != nil {
		return err
	}

	req, err := streamLoadRequest(ctx, e.cfg, e.cfg.Profiles, marshal, label)
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
		e.reporter.incrTotalRows(int64(len(profiles)))
		e.reporter.incrTotalBytes(int64(len(marshal)))

		if response.duplication() {
			e.logger.Warn("label already exists", zap.String("label", label), zap.Int("skipped", len(profiles)))
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
