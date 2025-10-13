package dorisexporter

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pprofiletest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.uber.org/zap"
)

func TestPushProfileData(t *testing.T) {
	port, err := findRandomPort()
	require.NoError(t, err)

	config := createDefaultConfig().(*Config)
	config.Endpoint = fmt.Sprintf("http://127.0.0.1:%d", port)
	config.CreateSchema = false
	require.NoError(t, config.Validate())

	logger := zap.NewNop()
	exporter := newProfilesExporter(logger, config, componenttest.NewNopTelemetrySettings())

	ctx := t.Context()

	client, err := createDorisHTTPClient(ctx, config, nil, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	require.NotNil(t, client)

	exporter.client = client

	defer func() {
		_ = exporter.shutdown(ctx)
	}()

	srvMux := http.NewServeMux()
	server := &http.Server{
		ReadTimeout: 3 * time.Second,
		Addr:        fmt.Sprintf(":%d", port),
		Handler:     srvMux,
	}

	go func() {
		tableSuffixes := []string{"sample", "stack"}
		for _, tableSuffix := range tableSuffixes {
			url := fmt.Sprintf("/api/otel/otel_profiles_%s/_stream_load", tableSuffix)
			srvMux.HandleFunc(url, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"Status":"Success"}`))
			})
		}
		err = server.ListenAndServe()
		assert.Equal(t, http.ErrServerClosed, err)
	}()

	err0 := errors.New("Not Started")
	for i := 0; err0 != nil && i < 10; i++ { // until server started
		err0 = exporter.pushProfilesData(ctx, simpleProfiles())
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err0)

	_ = server.Shutdown(ctx)
}

func simpleProfiles() pprofile.Profiles {
	profileID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	res := pcommon.NewResource()
	res.Attributes().PutEmpty("service.name").SetStr("test-service")
	res.Attributes().PutEmpty("host.name").SetStr("localhost")
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("scope")
	profiles := pprofiletest.Profiles{
		ResourceProfiles: []pprofiletest.ResourceProfile{
			{
				Resource: res,
				ScopeProfiles: []pprofiletest.ScopeProfile{
					{
						SchemaURL: "test_schema_url",
						Scope:     scope,
						Profiles: []pprofiletest.Profile{
							{
								Attributes: []pprofiletest.Attribute{
									{Key: "http.method", Value: "get"},
									{Key: "http.path", Value: "/health"},
									{Key: "http.url", Value: "http://localhost/health"},
									{Key: "flags", Value: "A|B|C"},
									{Key: "total.string", Value: "123456789"},
								},
								ProfileID:              profileID,
								TimeNanos:              1111,
								DurationNanos:          222,
								DroppedAttributesCount: 1,
								OriginalPayloadFormat:  "operationA",
								Sample: []pprofiletest.Sample{
									{
										Locations: []pprofiletest.Location{
											{
												Line: []pprofiletest.Line{
													{
														Function: pprofiletest.Function{
															Name: "json.Marshall",
														},
													},
													{
														Function: pprofiletest.Function{
															Name: "serializeResponse",
														},
													},
													{
														Function: pprofiletest.Function{
															Name: "handleRequest",
														},
													},
													{
														Function: pprofiletest.Function{
															Name: "main",
														},
													},
												},
											},
										},
									},
									{
										Locations: []pprofiletest.Location{
											{
												Line: []pprofiletest.Line{
													{
														Function: pprofiletest.Function{
															Name: "queryDatabase",
														},
													},
													{
														Function: pprofiletest.Function{
															Name: "handleRequest",
														},
													},
													{
														Function: pprofiletest.Function{
															Name: "main",
														},
													},
												},
											},
										},
									},
								},
							},
							{
								Attributes: []pprofiletest.Attribute{
									{Key: "http.method", Value: "get"},
									{Key: "http.path", Value: "/health"},
									{Key: "http.url", Value: "http://localhost/health"},
									{Key: "flags", Value: "C|D"},
									{Key: "total.string", Value: "345678"},
								},
								ProfileID:              profileID,
								TimeNanos:              3333,
								DurationNanos:          444,
								DroppedAttributesCount: 1,
								OriginalPayloadFormat:  "",
								Sample:                 []pprofiletest.Sample{},
							},
						},
					},
				},
			},
		},
	}

	return profiles.Transform()
}

func TestXxx(t *testing.T) {
	// 14695981039346656037
	stackHash := StackHash([]string{"", "", ""})
	fmt.Println(stackHash)
}
