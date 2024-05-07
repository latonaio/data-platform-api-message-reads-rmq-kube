package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-message-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-message-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var header *[]dpfm_api_output_formatter.Header
	for _, fn := range accepter {
		switch fn {
		case "Header":
			func() {
				header = c.Header(mtx, input, output, errs, log)
			}()
		case "HeadersBySenderReceiver":
			func() {
				header = c.HeadersBySenderReceiver(mtx, input, output, errs, log)
			}()
		case "HeadersBySender":
			func() {
				header = c.HeadersBySender(mtx, input, output, errs, log)
			}()
		case "HeadersByReceiver":
			func() {
				header = c.HeadersByReceiver(mtx, input, output, errs, log)
			}()
		default:
		}
		if len(*errs) != 0 {
			break
		}
	}

	data := &dpfm_api_output_formatter.Message{
		Header:				header,
	}

	return data
}

func (c *DPFMAPICaller) Header(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Message = %d", input.Header.Message)

	if input.Header.MessageIsSent != nil {
		where = fmt.Sprintf("%s\nAND header.MessageIsSent = %v", where, *input.Header.MessageIsSent)
	}
	
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_message_header_data AS header
		` + where + ` ORDER BY header.CreationDate ASC, header.CreationTime ASC, header.IsMarkedForDeletion ASC, header.MessageIsSent ASC, header.Message ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersBySenderReceiver(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {

	where := fmt.Sprintf("WHERE header.Sender = %d", input.Header.Sender)

	where := fmt.Sprintf("%s\nAND header.Receiver = %d", input.Header.Receiver)

	if input.Header.MessageIsSent != nil {
		where = fmt.Sprintf("%s\nAND header.MessageIsSent = %v", where, *input.Header.MessageIsSent)
	}
	
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_message_header_data AS header
		` + where + ` ORDER BY header.CreationDate ASC, header.CreationTime ASC, header.IsMarkedForDeletion ASC, header.MessageIsSent ASC, header.Receiver ASC, header.Sender ASC, header.Message ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersBySender(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {

	where := fmt.Sprintf("WHERE header.Sender = %d", input.Header.Sender)

	if input.Header.MessageIsSent != nil {
		where = fmt.Sprintf("%s\nAND header.MessageIsSent = %v", where, *input.Header.MessageIsSent)
	}
	
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_message_header_data AS header
		` + where + ` ORDER BY header.CreationDate ASC, header.CreationTime ASC, header.IsMarkedForDeletion ASC, header.MessageIsSent ASC, header.Receiver ASC, header.Sender ASC, header.Message ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersByReceiver(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {

	where := fmt.Sprintf("WHERE header.Receiver = %d", input.Header.Receiver)

	if input.Header.MessageIsSent != nil {
		where = fmt.Sprintf("%s\nAND header.MessageIsSent = %v", where, *input.Header.MessageIsSent)
	}

	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_message_header_data AS header
		` + where + ` ORDER BY header.CreationDate ASC, header.CreationTime ASC, header.IsMarkedForDeletion ASC, header.MessageIsSent ASC, header.Receiver ASC, header.Sender ASC, header.Message ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
