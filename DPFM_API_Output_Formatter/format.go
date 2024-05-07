package dpfm_api_output_formatter

import (
	"data-platform-api-message-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*[]Header, error) {
	defer rows.Close()
	header := make([]Header, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Header{}

		err := rows.Scan(
			&pm.Message,
			&pm.MessageType,
			&pm.Sender,
			&pm.Receiver,
			&pm.Language,
			&pm.Title,
			&pm.LongText,
			&pm.MessageIsSent,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.LastChangeDate,
			&pm.LastChangeTime,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}

		data := pm
		header = append(header, Header{
			Message:				data.Message,
			MessageType:			data.MessageType,
			Sender:					data.Sender,
			Receiver:				data.Receiver,
			Language:				data.Language,
			Title:					data.Title,
			LongText:				data.LongText,
			MessageIsSent:			data.MessageIsSent,
			CreationDate:			data.CreationDate,
			CreationTime:			data.CreationTime,
			LastChangeDate:			data.LastChangeDate,
			LastChangeTime:			data.LastChangeTime,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,

		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &header, nil
	}

	return &header, nil
}
