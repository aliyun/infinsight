package client

import (
	"os"
)

type FileClient struct {
	connectString string
	session       *os.File
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  NewMongoClient
//  Description:
// =====================================================================================
*/
func NewFileClient() ClientInterface {
	return new(FileClient)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ConnectString
//  Description:
// =====================================================================================
*/
func (client *FileClient) ConnectString(connectString string) ClientInterface {
	client.connectString = connectString
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Username
//  Description:
// =====================================================================================
*/
func (client *FileClient) Username(username string) ClientInterface {
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Password
//  Description:
// =====================================================================================
*/
func (client *FileClient) Password(password string) ClientInterface {
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  UseDB
//  Description:
// =====================================================================================
*/
func (client *FileClient) UseDB(db string) ClientInterface {
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetOpt
//  Description:  目前对mongo的参数配置并不是很熟悉，对此测试并不完全
//                目前只用到majority和Monotonic配置
// =====================================================================================
*/
func (client *FileClient) SetOpt(opts map[string]interface{}) ClientInterface {
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  EstablishConnect
//  Description:
// =====================================================================================
*/
func (client *FileClient) EstablishConnect() (ClientInterface, error) {
	var err error
	client.session, err = os.Open(client.connectString)
	return client, err
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Close
//  Description:
// =====================================================================================
*/
func (client *FileClient) Close() {
	client.session.Close()
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetSession
//  Description:
// =====================================================================================
*/
func (client *FileClient) GetSession() interface{} {
	return client.session
}