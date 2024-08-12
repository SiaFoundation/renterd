package api

type (
	// BuildState contains static information about the build.
	BuildState struct {
		Version   string      `json:"version"`
		Commit    string      `json:"commit"`
		OS        string      `json:"os"`
		BuildTime TimeRFC3339 `json:"buildTime"`
	}
)
