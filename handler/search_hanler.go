package handler

type SearchHandler struct {
	bucketName   string
	metadataKeys []string
}

func NewSearchHandler(bucketName string, metadataKeys []string) *SearchHandler {
	return &SearchHandler{
		bucketName:   bucketName,
		metadataKeys: metadataKeys,
	}
}

func (h *SearchHandler) Do() error {
	return nil
}
