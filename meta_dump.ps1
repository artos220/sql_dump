$DatabaseList = "";           #<--- Potential Input Parm - Default value: NULL (representing ALL databases)
$ServerName = "biload" #If you have a named instance, you should put the name. 
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$path  = Join-Path -Path $scriptDir -ChildPath \meta

$DebugPreference = "Continue" #SilentlyContinue

[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO') | out-null
$serverInstance = New-Object ('Microsoft.SqlServer.Management.Smo.Server') $ServerName
$IncludeTypes = @("Tables","StoredProcedures","Views","UserDefinedFunctions", "Triggers") #object you want do backup. 
$ExcludeSchemas = @("sys","Information_Schema")

$scripter = New-Object ('Microsoft.SqlServer.Management.Smo.Scripter') ($serverInstance);
$scripter.Options.AnsiFile = $false 
#$scripter.Options.Encoding = $ASCIIEncoding
$scripter.Options.IncludeHeaders = $false 
$scripter.Options.ScriptOwner = $false 
$scripter.Options.AppendToFile = $false 
$scripter.Options.AllowSystemobjects = $false 
$scripter.Options.ScriptDrops = $false 
$scripter.Options.WithDependencies = $false
$scripter.Options.SchemaQualify = $true 
$scripter.Options.SchemaQualifyForeignKeysReferences = $true
$scripter.Options.ScriptBatchTerminator = $false 
$scripter.Options.Indexes = $true 
$scripter.Options.ClusteredIndexes = $true
$scripter.Options.NonClusteredIndexes = $true
$scripter.Options.NoCollation = $true
$scripter.Options.DriAll = $true
$scripter.Options.DriIncludeSystemNames = $false
$scripter.Options.ToFileOnly = $true
$scripter.Options.Permissions = $true


if ($DatabaseList -eq "" -or $DatabaseList -eq $null)
    {$DatabaseNames = $null}
else
   {#The input for multiple entries is to be delimited with a semi-colon so it can be loaded into the variable as an array.
    $DatabaseNames = $DatabaseList.split(";")
   }; 
   
function getDatabases ($SQL_Server)
{ 
	# Bypass off-line databases i.e. EQUAL to "Normal" and databases that are not a Snapshot Database (i.e. IsDatabaseSnapshot <> True )
    # as well as filtering out the System Databases.
    $databases = $SQL_Server.Databases | Where-Object {$_.IsSystemObject -eq $false -and ($DatabaseNames -contains $_.Name -or $DatabaseNames -eq $null -and $_.Status -eq "Normal" -and $_.IsDatabaseSnapshot -ne $true)}; 
    return $databases; 
} 


$databases = getDatabases $serverInstance; 

foreach ($db in $databases)
{
       $dbname = "$db".replace("[","").replace("]","")
       $dbpath = "$path"+ "\"+"$dbname" + "\"
    if ( !(Test-Path $dbpath))
           {$null=new-item -type directory -name "$dbname"-path "$path"}
 
       foreach ($Type in $IncludeTypes)
       {
              $objpath = "$dbpath" + "$Type" + "\"
         if ( !(Test-Path $objpath))
           {$null=new-item -type directory -name "$Type"-path "$dbpath"}
              foreach ($objs in $db.$Type)
              {
                     If ($ExcludeSchemas -notcontains $objs.Schema ) 
                      {
                           $ObjName = "$objs".replace("[","").replace("]","")                  
                           $OutFile = "$objpath" + "$ObjName" + ".sql"
                           $scripter.Options.FileName = "$OutFile";
                           Write-Debug $OutFile

                           $scripter.Script($objs)
                      }
              }
       }     
}
