package api

type WebHookRegisterRequest struct {
	Event  string `json:"event"`
	Module string `json:"module"`
	URL    string `json:"url"`
}
