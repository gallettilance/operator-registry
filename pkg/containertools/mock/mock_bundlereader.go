// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/operator-framework/operator-registry/pkg/containertools (interfaces: BundleReader)

// Package mock is a generated GoMock package.
package mock

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// ContainerToolsBundleReader is a mock of BundleReader interface
type ContainerToolsBundleReader struct {
	ctrl     *gomock.Controller
	recorder *ContainerToolsBundleReaderMockRecorder
}

// ContainerToolsBundleReaderMockRecorder is the mock recorder for ContainerToolsBundleReader
type ContainerToolsBundleReaderMockRecorder struct {
	mock *ContainerToolsBundleReader
}

// NewContainerToolsBundleReader creates a new mock instance
func NewContainerToolsBundleReader(ctrl *gomock.Controller) *ContainerToolsBundleReader {
	mock := &ContainerToolsBundleReader{ctrl: ctrl}
	mock.recorder = &ContainerToolsBundleReaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *ContainerToolsBundleReader) EXPECT() *ContainerToolsBundleReaderMockRecorder {
	return m.recorder
}

// GetBundle mocks base method
func (m *ContainerToolsBundleReader) GetBundle(arg0, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBundle", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// GetBundle indicates an expected call of GetBundle
func (mr *ContainerToolsBundleReaderMockRecorder) GetBundle(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBundle", reflect.TypeOf((*ContainerToolsBundleReader)(nil).GetBundle), arg0, arg1)
}