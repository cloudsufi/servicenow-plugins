# ServiceNow Batch Sink

Description
-----------

Writes to the specified table within ServiceNow. All the fields in the source table must match with the fields in the 
destination table. For update operations, sys_id must be present.

Properties
----------

**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Table Name**: The name of the ServiceNow table into which data is to be pushed.

**Client ID**: The Client ID for ServiceNow Instance.

**Client Secret**: The Client Secret for ServiceNow Instance.

**REST API Endpoint**: The REST API Endpoint for ServiceNow Instance. For example, `https://instance.service-now.com`

**User Name**: The user name for ServiceNow Instance.

**Password**: The password for ServiceNow Instance.

**Operation** The type of operation to be performed. Insert operation will insert the data. Update operation will update
existing data in the table. "sys_id" must be present in the records.

**Page Size** No. of requests that will be sent to ServiceNow Batch API as a payload. Rest API property in Transaction 
quota section "REST Batch API request timeout" should be increased to use higher records in a batch. By default this 
property has a value of 30 sec which can handle approximately 200 records in a batch. To use a bigger page size set it 
to a higher value.

Data Types Mapping
----------

    | ServiceNow Data Type           | CDAP Schema Data Type | Comment                                            |
    | ------------------------------ | --------------------- | -------------------------------------------------- |
    | decimal                        | double                |                                                    |
    | integer                        | int                   |                                                    |
    | boolean                        | boolean               |                                                    |
    | reference                      | string                |                                                    |
    | currency                       | string                |                                                    |
    | glide_date                     | string                |                                                    |
    | glide_date_time                | string                |                                                    |
    | sys_class_name                 | string                |                                                    |
    | domain_id                      | string                |                                                    |
    | domain_path                    | string                |                                                    |
    | guid                           | string                |                                                    |
    | translated_html                | string                |                                                    |
    | journal                        | string                |                                                    |
    | string                         | string                |                                                    |
