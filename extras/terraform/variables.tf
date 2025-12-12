variable "location" {
    description = "The location where resources will be created"
    type        = string
    default     = "eastus2"
}


variable "prefix" {
    description = "Prefix for naming resources"
    type        = string
    default     = "example-dev-etl"
}


variable "key_vault_name" { 
    description = "The name of the Key Vault"
    type        = string
    default     = "databricks-kv-secret"
}


variable "pass_db_sql" {
  description = "Password for the SQL admin user"
  type        = string
  default = "example@"

}