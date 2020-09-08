#Sign in to Azure
Connect-AzureRmAccount

#Create a resource group
New-AzureRmResourceGroup -Name TutorialResources -Location eastus

#Create admin credentials for the VM
$cred = Get-Credential -Message "Enter a username and password for the virtual machine."

#Create a virtual machine
$vmParams = @{
  ResourceGroupName = 'TutorialResources'
  Name = 'TutorialVM1'
  Location = 'eastus'
  ImageName = 'Win2016Datacenter'
  PublicIpAddressName = 'tutorialPublicIp'
  Credential = $cred
  OpenPorts = 3389
}
$newVM1 = New-AzureRmVM @vmParams

#Get VM information with queries
$newVM1.OSProfile | Select-Object ComputerName,AdminUserName
$newVM1 | Get-AzureRmNetworkInterface |
  Select-Object -ExpandProperty IpConfigurations |
    Select-Object Name,PrivateIpAddress

#Public IP address
$publicIp = Get-AzureRmPublicIpAddress -Name tutorialPublicIp -ResourceGroupName TutorialResources
$publicIp | Select-Object Name,IpAddress,@{label='FQDN';expression={$_.DnsSettings.Fqdn}}

#Connect VM over Remote Desktop
mstsc.exe /v <PUBLIC_IP_ADDRESS>

#Creating a new VM on the existing subnet
$vm2Params = @{
  ResourceGroupName = 'TutorialResources'
  Name = 'TutorialVM2'
  ImageName = 'Win2016Datacenter'
  VirtualNetworkName = 'TutorialVM1'
  SubnetName = 'TutorialVM1'
  PublicIpAddressName = 'tutorialPublicIp2'
  Credential = $cred
  OpenPorts = 3389
}
$newVM2 = New-AzureRmVM @vm2Params

#Connect
mstsc.exe /v $newVM2.FullyQualifiedDomainName

#Cleanup
$job = Remove-AzureRmResourceGroup -Name TutorialResources -Force -AsJob
$job
Wait-Job -Id $job.Id