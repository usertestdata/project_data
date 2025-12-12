resource "azurerm_resource_group" "example" {
    location = var.location
    name = var.prefix
}


resource "azurerm_storage_account" "adl" {
 name = "${replace(var.prefix, "-", "sdl")}"
 resource_group_name = azurerm_resource_group.example.name
 location =  var.location
 account_tier = "Standard"
 account_replication_type = "LRS"
 is_hns_enabled = true
 min_tls_version = "TLS1_2"

}


resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id = azurerm_storage_account.adl.id
  container_access_type = "private" 
}


resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id = azurerm_storage_account.adl.id
  container_access_type = "private"
}


resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_id = azurerm_storage_account.adl.id
  container_access_type = "private"
}


resource "azurerm_storage_container" "metastore_container" {
  name                  = "metastore"
  storage_account_id = azurerm_storage_account.adl.id
  container_access_type = "private"
}


data "azurerm_client_config" "current" {}

locals {
  contributor_role_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c"
}


resource "azurerm_databricks_workspace" "dbr" {
  name = "${var.prefix}-databricks"
  resource_group_name = azurerm_resource_group.example.name
  location = var.location
  sku = "premium" 

}



resource "azurerm_data_factory" "adf" {
    name                = "${var.prefix}-adf"
    location            = var.location
    resource_group_name = azurerm_resource_group.example.name

  identity {type = "SystemAssigned"}
}


resource "azurerm_role_assignment" "adf_to_databricks_role" {
    scope                = azurerm_databricks_workspace.dbr.id
    role_definition_id   = local.contributor_role_id
    principal_id         = azurerm_data_factory.adf.identity[0].principal_id

    depends_on = [
        azurerm_data_factory.adf,
        azurerm_databricks_workspace.dbr
    ]
}

resource "azurerm_user_assigned_identity" "databricks_mi" {
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  name                = "${var.prefix}-databricks-umi"
}


resource "azurerm_databricks_access_connector" "connector" {
  name                = "${var.prefix}-databricks-connector"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location

  identity {
    type = "SystemAssigned, UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.databricks_mi.id]
  }
}


resource "azurerm_role_assignment" "databricks_storage_access" {
  scope                = azurerm_storage_account.adl.id
  role_definition_name = "Storage Blob Data Contributor" 
  principal_id         = azurerm_user_assigned_identity.databricks_mi.principal_id
}



resource "azurerm_data_factory_linked_service_azure_databricks" "databricks_ls" {
    name                 = "${var.prefix}-databricks-ls"
    data_factory_id      = azurerm_data_factory.adf.id
    adb_domain = azurerm_databricks_workspace.dbr.workspace_url
    
  msi_work_space_resource_id = azurerm_databricks_workspace.dbr.id


  new_cluster_config {
    cluster_version = "13.3.x-scala2.12"
    node_type = "Standard_DS3_v2"
    max_number_of_workers = 1
  }
   
}


resource "azurerm_data_factory_pipeline" "notebook_pipeline" {
  name                = "PL_Ejecutar_Notebooks"
  data_factory_id     = azurerm_data_factory.adf.id

 
  activities_json = jsonencode([
   
    {
      "name": "NotebookActivity_1_Ingesta",
      "type": "DatabricksNotebook",
      "linkedServiceName": {
        "referenceName": azurerm_data_factory_linked_service_azure_databricks.databricks_ls.name,
        "type": "LinkedServiceReference"
      },
      "typeProperties": {
        "notebookPath": "/Shared/data_factory_notebooks/01_Ingesta"
      }
    },
   
    {
      "name": "NotebookActivity_2_Transformacion",
      "type": "DatabricksNotebook",
      "linkedServiceName": {
        "referenceName": azurerm_data_factory_linked_service_azure_databricks.databricks_ls.name,
        "type": "LinkedServiceReference"
      },
      "typeProperties": {
        "notebookPath": "/Shared/data_factory_notebooks/02_Transformacion"
      },
      "dependsOn": [
        {
          "activity": "NotebookActivity_1_Ingesta",
          "dependencyConditions": ["Succeeded"]
        }
      ]
    },
   
    {
      "name": "NotebookActivity_3_Carga",
      "type": "DatabricksNotebook",
      "linkedServiceName": {
        "referenceName": azurerm_data_factory_linked_service_azure_databricks.databricks_ls.name,
        "type": "LinkedServiceReference"
      },
      "typeProperties": {
        "notebookPath": "/Shared/data_factory_notebooks/03_Carga_Final"
      },
      "dependsOn": [
        {
          "activity": "NotebookActivity_2_Transformacion",
          "dependencyConditions": ["Succeeded"]
        }
      ]
    }
  ])
}


resource "azurerm_eventhub_namespace" "eventhub_ns" {
    name                = "${var.prefix}-eh-ns"
    location            = azurerm_resource_group.example.location
    resource_group_name = azurerm_resource_group.example.name
    sku                 = "Basic"
    capacity            = 1
}


resource "azurerm_eventhub" "event_ingestion" {
    name                = "event-ingestion"
    namespace_id = azurerm_eventhub_namespace.eventhub_ns.id
    partition_count     = 2
    message_retention   = 1
    
}


resource "azurerm_key_vault" "kv" {
  name                = "${var.prefix}" 
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
  sku_name            = "standard"
  tenant_id = data.azurerm_client_config.current.tenant_id

}


resource "azurerm_key_vault_access_policy" "terraform_deployer_access" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  

  object_id    = data.azurerm_client_config.current.object_id 

  secret_permissions = [
    "Get",  
    "List", 
    "Set",  
    "Delete",
    "Purge"
  ]
}


resource "azurerm_key_vault_secret"  "sql_pwd" {
    name         = "${var.prefix}-sql-admin-ptt"
    value        = var.pass_db_sql
    key_vault_id = azurerm_key_vault.kv.id

    depends_on = [
    azurerm_key_vault_access_policy.terraform_deployer_access
  ]
}



