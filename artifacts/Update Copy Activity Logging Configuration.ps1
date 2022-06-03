#Requires -Module MiniGraph

#Parameters section. Manually populate these with the values from your environment.
Param(
    $ResourceGroup = "<ADD YOUR RESOURCE GROUP NAME HERE>",                 #This reflects the Resource Group where Azure Data Factory is hosted at.
    $ADFName = "<ADF RESOURCE NAME>",                                       #This reflects the name of your Azure Data Factory resource.
    $NewLogDestinationName = "LS_ADLSGen2_New",                             #This is the name of the new Log destination (ADLS Gen 2 Account) Linked Service.
    $CopyActivityLogDestinationPath = "log/copyactivity/",                  #This is the path within the Log Destination above where the logs are being written to.
    $ADFPipelineBackupOutputFilePath = "C:\adf\Pipelines\Backup\",          #This is the local path where the Azure Data Factory Pipelines can be backed-up to, before the changes - in case you need to rollback your changes (as JSON).
    $ADFPipelineOutputFilePath = "C:\adf\Pipelines\Results\",               #This is the local path where the Azure Data Factory Pipelines can be backed-up to, after the changes (as JSON).
    $KeyVaultName = "<ADD YOUR KEY VAULT NAME HERE>",                       #This is the name of your Key Vault, where your Tenant ID, Client ID and Client Secret will be stored.
    $SecretNameTenantID = "XXXXX-TENANT-ID",                                #This is the Tenant ID secret name (not the value), as stored in the Key Vault 
    $SecretNameClientID = "XXXXX-ADF-APP-ID",                               #This is the App ID secret name (not the value), as stored in the Key Vault 
    $SecretNameClientSecret = "XXXXX-ADF-SECRET",                           #This is the Client Secret secret name (not the value), as stored in the Key Vault 
    $SecretNameSubscriptionID = "XXXXX-SUBSCRIPTION-ID"                     #This is the Subscription ID secret name (not the value), as stored in the Key Vault 
)

#Manually connect to Azure once, in order to authenticate and be able to retrieve Secrets from the Key Vault. 
#The connection to the Key Vault will take place under the context of the current user running the PowerShell session. 
    Connect-AzAccount

#Parameters Section
#Parameters populated with values retrieved from Azure Key Vault
    $TenantID = Get-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretNameTenantID -AsPlainText
    $ClientID = Get-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretNameClientID -AsPlainText
    $ClientSecret = (Get-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretNameClientSecret -AsPlainText | ConvertTo-SecureString -AsPlainText -Force)
    $SubscriptionID = Get-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretNameSubscriptionID -AsPlainText

#Set Graph Endpoint, used in the authentication below. 
    Set-GraphEndpoint -Url https://management.azure.com/

#Authenticates against Azure using the App ID and Secret above.
    Try {
        Connect-GraphClientSecret -ClientID $ClientID -TenantID $TenantID -ClientSecret $ClientSecret -Scopes 'https://management.azure.com/user_impersonation' -Resource https://management.azure.com/
    }
    Catch {
        Write-Output "StatusCode:" $_.Exception.Response.StatusCode.value__ 
        Write-Output "StatusDescription:" $_.Exception.Response.StatusDescription
        Throw
    }

#Call to the Azure Data Factory Pipelines API. This retrieves a list of existing Pipelines. 
    $APIGetPipelinesQuery = "subscriptions/$SubscriptionID/resourceGroups/$ResourceGroup/providers/Microsoft.DataFactory/factories/$ADFName/pipelines?api-version=2018-06-01"
    $InvokeGetPipelines = Invoke-GraphRequest -Query $APIGetPipelinesQuery

#This is the main/parent For Each Loop process. 
#This parent loop will loop through each Azure Data Factory Pipeline.
    ForEach ($ADFPipeline in $InvokeGetPipelines) {
        Try {
            #Retrieves the JSON structure for each Azure Data Factory pipeline previously retrieved on the previous step.
            $APIGetADFPipelines = "subscriptions/$SubscriptionID/resourceGroups/$ResourceGroup/providers/Microsoft.DataFactory/factories/$ADFName/pipelines/$($ADFPipeline.name)?api-version=2018-06-01"
            $InvokeGetADFPipelines = Invoke-GraphRequest -Query $APIGetADFPipelines -Method Get
            
            #These two next steps will backup the Pipelines as .json files into a local directory.
            #Change the $ADFPipelineOutputFilePath variable up above to change the backup destination directory. 
            #Comment out the next two lines if you want to skip this step.
            $ADFPipelineBackupOutputFile = $ADFPipelineBackupOutputFilePath + $ADFPipeline.name + ".json"
            $InvokeGetADFPipelines | ConvertTo-Json -Depth 100 | Set-Content -Path $ADFPipelineBackupOutputFile
            
            #This is the first child For Each Loop process. 
            #This child loop will loop through each Azure Data Factory Pipeline Copy Activity.
            ForEach ($ADFPipelineActivity in $ADFPipeline | Where-Object { $_.properties.activities.type -eq "Copy" }) {
                
                #The next steps inside this ForEach Loop will update the Logging Settings in each of the Copy activities within your Azure Data Factory Pipelines. 
                
                #Set enableSkipIncompatibleRow = True
                $settings = $ADFPipelineActivity.properties.activities.typeProperties | Where-Object {
                    $_.PSObject.Properties.Name -contains "enableSkipIncompatibleRow"
                }
                foreach ($setting in $settings) { $setting.enableSkipIncompatibleRow = $true }

                #Set enableCopyActivityLog = True
                $settings = $ADFPipelineActivity.properties.activities.typeProperties.logSettings | Where-Object {
                    $_.PSObject.Properties.Name -contains "enableCopyActivityLog"
                }
                foreach ($setting in $settings) { $setting.enableCopyActivityLog = $true }

                #Set logLevel = True
                $settings = $ADFPipelineActivity.properties.activities.typeProperties.logSettings.copyActivityLogSettings | Where-Object {
                    $_.PSObject.Properties.Name -contains "logLevel"
                }
                foreach ($setting in $settings) { $setting.logLevel = $true }

                #Set enableReliableLogging = True
                $settings = $ADFPipelineActivity.properties.activities.typeProperties.logSettings.copyActivityLogSettings | Where-Object {
                    $_.PSObject.Properties.Name -contains "enableReliableLogging"
                }
                foreach ($setting in $settings) { $setting.enableReliableLogging = $true }

                #Set referenceName = $NewLogDestinationName (Configured up above on the parameters area)
                #$NewLogDestinationName = 'LS_ADLSGen2' #Used for testing purposes only. REMOVE
                $settings = $ADFPipelineActivity.properties.activities.typeProperties.logSettings.logLocationSettings.linkedServiceName | Where-Object {
                    $_.PSObject.Properties.Name -contains "referenceName"
                }
                foreach ($setting in $settings) { $setting.referenceName = $NewLogDestinationName }

                #Set referenceName = $NewLogDestinationName (Configured up above on the parameters area)
                $settings = $ADFPipelineActivity.properties.activities.typeProperties.logSettings.logLocationSettings | Where-Object {
                    $_.PSObject.Properties.Name -contains "path"
                }
                foreach ($setting in $settings) { $setting.path = $CopyActivityLogDestinationPath }
                
                #Send the content of $ADFPipelineActivity to $ADFPipelineOutputFile output
                $ADFPipelineOutputFile = $ADFPipelineOutputFilePath + $ADFPipeline.name + ".json"
                $ADFPipelineActivity | ConvertTo-Json -Depth 32 | Set-Content -Path $ADFPipelineOutputFile
        
                #Exclude properties
                $ADFPipelineFiltered = $ADFPipelineActivity | Select-Object -Property * -ExcludeProperty id, name, type
                $ADFPipelineFilteredJSON = $ADFPipelineFiltered | ConvertTo-JSON -Depth 32
                
                #Invoke Azure Data Factory Update/Crete API
                $APIUpdateADFPipelines = "subscriptions/$SubscriptionID/resourceGroups/$ResourceGroup/providers/Microsoft.DataFactory/factories/$ADFName/pipelines/$($ADFPipeline.name)?api-version=2018-06-01"
                $InvokeGetADFPipelines = Invoke-GraphRequest -Query $APIUpdateADFPipelines -Method Put -Body $ADFPipelineFilteredJSON
            }
        }
        Catch {
            Write-Output "StatusCode:" $_.Exception.Response.StatusCode.value__ 
            Write-Output "StatusDescription:" $_.Exception.Response.StatusDescription
            Write-Error $_.ErrorDetails.Message
        }
    }
