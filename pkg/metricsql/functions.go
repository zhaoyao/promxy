package metricsql

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"math"
	"sort"
	"strconv"
	"strings"
)

func init() {
	// 注册 metricsql 函数

	parser.Functions["prometheus_buckets"] = &parser.Function{
		Name:       "prometheus_buckets",
		ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
		ReturnType: parser.ValueTypeVector,
	}
	promql.FunctionCalls["prometheus_buckets"] = prometheusBucketsFunc
}

// xxx_buckets { vmrange="a...b" } 1
// xxx_buckets { vmrange="c...d" } 1
func prometheusBucketsFunc(vals []parser.Value, args parser.Expressions, enh *promql.EvalNodeHelper) promql.Vector {
	vec := vals[0].(promql.Vector)
	type x struct {
		startStr string
		endStr   string
		start    float64
		end      float64
		ts       promql.Sample
	}

	var xss []x
	for _, el := range vec {
		vmrange := el.Metric.Get("vmrange")
		if len(vmrange) == 0 {
			if len(el.Metric.Get("le")) > 0 {
				enh.Out = append(enh.Out, el)
			}
			continue
		}

		n := strings.Index(vmrange, "...")
		if n < 0 {
			continue
		}
		startStr := vmrange[:n]
		start, err := strconv.ParseFloat(startStr, 64)
		if err != nil {
			continue
		}
		endStr := vmrange[n+len("..."):]
		end, err := strconv.ParseFloat(endStr, 64)
		if err != nil {
			continue
		}

		xss = append(xss, x{
			startStr: startStr,
			endStr:   endStr,
			start:    start,
			end:      end,
			ts:       el,
		})
	}

	sort.Slice(xss, func(i, j int) bool { return xss[i].end < xss[j].end })

	var prevX x
	for _, xs := range xss {

		if xs.ts.Point.V == 0 {
			// Skip buckets with zero values - they will be merged into a single bucket
			// when the next non-zero bucket appears.
			continue
		}

		if xs.start != prevX.end {
			// There is a gap between the previous bucket and the current bucket
			// or the previous bucket is skipped because it was zero.
			// Fill it with a time series with le=xs.start.
			enh.Out = append(enh.Out, promql.Sample{
				Point:  prevX.ts.Point,
				Metric: labels.NewBuilder(xs.ts.Metric).Del("vmrange").Set("le", xs.startStr).Labels(),
			})
		}

		tmp := xs.ts
		tmp.Point.V = prevX.ts.V + xs.ts.V
		prevX = xs
		prevX.ts = tmp

		enh.Out = append(enh.Out, promql.Sample{
			Point:  tmp.Point,
			Metric: labels.NewBuilder(xs.ts.Metric).Del("vmrange").Set("le", xs.endStr).Labels(),
		})

	}

	if !math.IsInf(prevX.end, 1) && prevX.ts.Point.V != 0 {
		enh.Out = append(enh.Out, promql.Sample{
			Point:  prevX.ts.Point,
			Metric: labels.NewBuilder(prevX.ts.Metric).Del("vmrange").Set("le", "+Inf").Labels(),
		})
	}

	return enh.Out
}
