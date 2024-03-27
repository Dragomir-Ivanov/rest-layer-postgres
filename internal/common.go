package internal

func CopyRow(row map[string]any) map[string]any {
	out := make(map[string]any)
	for k, v := range row {
		out[k] = v
	}
	return out
}
