package model

import (
	"time"
	
	go_core_db_pg 		"github.com/eliezerraj/go-core/database/postgre"
	go_core_otel_trace "github.com/eliezerraj/go-core/otel/trace"
)

type AppServer struct {
	Application 	*Application	 				`json:"application"`
	Server     		*Server     					`json:"server"`
	EnvTrace		*go_core_otel_trace.EnvTrace	`json:"env_trace"`
	DatabaseConfig	*go_core_db_pg.DatabaseConfig  	`json:"database_config"`
	Endpoint 		*[]Endpoint						`json:"endpoints"`
}

type MessageRouter struct {
	Message			string `json:"message"`
}

type Application struct {
	Name				string 	`json:"name"`
	Version				string 	`json:"version"`
	Account				string 	`json:"account,omitempty"`
	OsPid				string 	`json:"os_pid"`
	IPAddress			string 	`json:"ip_address"`
	Env					string 	`json:"enviroment,omitempty"`
	LogLevel			string 	`json:"log_level,omitempty"`
	OtelTraces			bool   	`json:"otel_traces"`
	OtelMetrics			bool   	`json:"otel_metrics"`
	OtelLogs			bool   	`json:"otel_logs"`
	StdOutLogGroup 		bool   	`json:"stdout_log_group"`
	LogGroup			string 	`json:"log_group,omitempty"`
}

type Server struct {
	Port 			int `json:"port"`
	ReadTimeout		int `json:"readTimeout"`
	WriteTimeout	int `json:"writeTimeout"`
	IdleTimeout		int `json:"idleTimeout"`
	CtxTimeout		int `json:"ctxTimeout"`
}

type Endpoint struct {
	Name			string `json:"name_service"`
	Url				string `json:"url"`
	Method			string `json:"method"`
	XApigwApiId		string `json:"x-apigw-api-id,omitempty"`
	HostName		string `json:"host_name"`
	HttpTimeout		time.Duration `json:"httpTimeout"`
}


type Product struct {
	ID			int			`json:"id,omitempty"`
	Sku			string		`json:"sku,omitempty"`
	Type		string 		`json:"type,omitempty"`
	Name		string 		`json:"name,omitempty"`
	CreatedAt	time.Time 	`json:"created_at,omitempty"`
	UpdatedAt	*time.Time 	`json:"update_at,omitempty"`	
}

type Cart struct {
	ID				int			`json:"id,omitempty"`
	UserId			string		`json:"user_id,omitempty"`
	CartItem 		*[]CartItem	`json:"cart_item,omitempty"` 	
	CreatedAt		time.Time 	`json:"created_at,omitempty"`
	UpdatedAt		*time.Time 	`json:"update_at,omitempty"`	
}

type CartItem struct {
	ID				int	`json:"id,omitempty"`
	Product 		Product	 `json:"product"`
	Quantity		int	`json:"quantity,omitempty"`
	Discount		float64	`json:"discount,omitempty"`
	Price			float64	`json:"price,omitempty"`
	CreatedAt		time.Time 	`json:"created_at,omitempty"`
	UpdatedAt		*time.Time 	`json:"update_at,omitempty"`	
}