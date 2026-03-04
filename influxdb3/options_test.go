package influxdb3

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestQueryOptions(t *testing.T) {
	fn := func(options ...QueryOption) *QueryOptions {
		return newQueryOptions(&DefaultQueryOptions, options)
	}
	va := func(options ...QueryOption) []QueryOption {
		return options
	}

	testCases := []struct {
		name string
		opts []QueryOption
		want *QueryOptions
	}{
		{
			name: "default",
			opts: nil,
			want: &DefaultQueryOptions,
		},
		{
			name: "override database",
			opts: va(WithDatabase("db-x")),
			want: &QueryOptions{
				Database: "db-x",
			},
		},
		{
			name: "override database and query type",
			opts: va(WithDatabase("db-x"), WithQueryType(InfluxQL)),
			want: &QueryOptions{
				Database:  "db-x",
				QueryType: InfluxQL,
			},
		},
		{
			name: "add header",
			opts: va(WithHeader("header-a", "value-a")),
			want: &QueryOptions{
				Headers: http.Header{
					"header-a": {"value-a"},
				},
			},
		},
		{
			name: "add headers",
			opts: va(WithHeader("header-a", "value-a"), WithHeader("header-b", "value-b")),
			want: &QueryOptions{
				Headers: http.Header{
					"header-a": {"value-a"},
					"header-b": {"value-b"},
				},
			},
		},
		{
			name: "add grpc option",
			opts: va(WithGrpcCallOption(grpc.MaxCallRecvMsgSize(16)),
				WithGrpcCallOption(grpc.MaxCallSendMsgSize(16)),
				WithGrpcCallOption(grpc.WaitForReady(true)),
				WithGrpcCallOption(grpc.StaticMethod()),
				WithGrpcCallOption(grpc.Header(&metadata.MD{"meta-key": []string{"meta-value1", "meta-value2"}})),
			),
			want: &QueryOptions{
				GrpcCallOptions: []grpc.CallOption{
					grpc.MaxCallRecvMsgSize(16),
					grpc.MaxCallSendMsgSize(16),
					grpc.WaitForReady(true),
					grpc.StaticMethod(),
					grpc.Header(&metadata.MD{"meta-key": []string{"meta-value1", "meta-value2"}}),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := fn(tc.opts...)
			if diff := cmp.Diff(tc.want, options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestWriteOptions(t *testing.T) {
	fn := func(options ...WriteOption) *WriteOptions {
		return newWriteOptions(&DefaultWriteOptions, options)
	}
	va := func(options ...WriteOption) []WriteOption {
		return options
	}

	testCases := []struct {
		name string
		opts []WriteOption
		want *WriteOptions
	}{
		{
			name: "default",
			want: &DefaultWriteOptions,
		},
		{
			name: "default",
			opts: nil,
			want: &DefaultWriteOptions,
		},
		{
			name: "override database",
			opts: va(WithDatabase("db-x")),
			want: &WriteOptions{
				Database:      "db-x",
				Precision:     DefaultWriteOptions.Precision,
				GzipThreshold: DefaultWriteOptions.GzipThreshold,
				NoSync:        DefaultWriteOptions.NoSync,
			},
		},
		{
			name: "override database and precision",
			opts: va(WithDatabase("db-x"), WithPrecision(Millisecond)),
			want: &WriteOptions{
				Database:      "db-x",
				Precision:     Millisecond,
				GzipThreshold: DefaultWriteOptions.GzipThreshold,
				NoSync:        DefaultWriteOptions.NoSync,
			},
		},
		{
			name: "override database, precision, GZIP threshold, write no sync and accept partial",
			opts: va(
				WithDatabase("db-x"),
				WithPrecision(Millisecond),
				WithGzipThreshold(4096),
				WithNoSync(true),
				WithAcceptPartial(true),
			),
			want: &WriteOptions{
				Database:      "db-x",
				Precision:     Millisecond,
				GzipThreshold: 4096,
				NoSync:        true,
				AcceptPartial: true,
			},
		},
		{
			name: "override tag order",
			opts: va(WithTagOrder("region", "host")),
			want: &WriteOptions{
				Precision:     DefaultWriteOptions.Precision,
				TagOrder:      []string{"region", "host"},
				GzipThreshold: DefaultWriteOptions.GzipThreshold,
				NoSync:        DefaultWriteOptions.NoSync,
			},
		},
		{
			name: "override accept partial",
			opts: va(WithAcceptPartial(true)),
			want: &WriteOptions{
				Precision:     DefaultWriteOptions.Precision,
				GzipThreshold: DefaultWriteOptions.GzipThreshold,
				NoSync:        DefaultWriteOptions.NoSync,
				AcceptPartial: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := fn(tc.opts...)
			if diff := cmp.Diff(tc.want, options); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestWithTagOrderCopiesInput(t *testing.T) {
	order := []string{"region", "host"}
	options := newWriteOptions(&DefaultWriteOptions, []WriteOption{WithTagOrder(order...)})
	order[0] = "mutated"

	if diff := cmp.Diff([]string{"region", "host"}, options.TagOrder); diff != "" {
		t.Fatal(diff)
	}
}

func TestWithDefaultTagsCopiesInput(t *testing.T) {
	tags := map[string]string{
		"region": "us-east",
		"host":   "h1",
	}
	options := newWriteOptions(&DefaultWriteOptions, []WriteOption{WithDefaultTags(tags)})

	tags["region"] = "eu-west"
	tags["rack"] = "r1"

	if diff := cmp.Diff(
		map[string]string{"region": "us-east", "host": "h1"},
		options.DefaultTags,
	); diff != "" {
		t.Fatal(diff)
	}
}
