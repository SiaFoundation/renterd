package api

type (
	// BuildState contains static information about the build.
	BuildState struct {
		Network   string      `json:"network"`
		Version   string      `json:"version"`
		Commit    string      `json:"commit"`
		OS        string      `json:"OS"`
		BuildTime TimeRFC3339 `json:"buildTime"`
	}
)
