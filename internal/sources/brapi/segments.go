package brapi

import "strings"

// normalizeSegment maps brapi's free-form sector labels into the
// canonical segment vocabulary used by the MCP server. Unknown labels
// map to "other".
func normalizeSegment(sector string) string {
	s := strings.ToLower(strings.TrimSpace(sector))
	switch {
	case s == "":
		return "other"
	case containsAny(s, "log", "industrial"):
		return "logistic"
	case containsAny(s, "lajes", "office", "corp"):
		return "office"
	case containsAny(s, "shop", "retail", "varejo"):
		return "retail"
	case containsAny(s, "papel", "paper", "cri", "recebív"):
		return "paper"
	case containsAny(s, "híbrid", "hibrid", "hybrid", "diversif"):
		return "hybrid"
	case containsAny(s, "fundo de fund", "fof"):
		return "fof"
	case containsAny(s, "residen", "residential"):
		return "residential"
	case containsAny(s, "saúde", "saude", "health", "hospital"):
		return "healthcare"
	}
	return "other"
}

// mandateFromSegment derives the fund mandate (brick/paper/hybrid/fof)
// from the segment heuristic above.
func mandateFromSegment(sector string) string {
	switch normalizeSegment(sector) {
	case "paper":
		return "paper"
	case "fof":
		return "fof"
	case "hybrid":
		return "hybrid"
	default:
		return "brick"
	}
}

func containsAny(haystack string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(haystack, n) {
			return true
		}
	}
	return false
}
