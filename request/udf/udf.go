package udf

type UDFTabCol struct {
	LeftTableId               string   `json:"leftTableId,omitempty"`
	LeftTableName             string   `json:"leftTableName,omitempty"` // tableId 和 tableName 区别见 DataSource。name 可以不填
	LeftColumnNames           []string `json:"leftColumnNames,omitempty"`
	RightTableId              string   `json:"rightTableId,omitempty"`              // 左右列相同时，不传即可
	RightTableName            string   `json:"rightTableName,omitempty"`            // 左右列相同时，不传即可
	RightColumnNames          []string `json:"rightColumnNames,omitempty"`          // 左右列相同时，不传即可
	Type                      string   `json:"type,omitempty"`                      // similar/ML
	Name                      string   `json:"name,omitempty"`                      // 比如 jaccard sentence-bert
	Threshold                 float64  `json:"threshold,omitempty"`                 // 阈值，只在相似度时有用
	LeftColumnVectorFilePath  string   `json:"leftColumnVectorFilePath,omitempty"`  // 向量文件 HDFS 地址。只有 ML 时才有用
	RightColumnVectorFilePath string   `json:"rightColumnVectorFilePath,omitempty"` // 向量文件 HDFS 地址。左右列相同时，不传即可
}
