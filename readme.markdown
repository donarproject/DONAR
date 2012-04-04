DONAR Code
=========================
This is a preliminary commit which open sources some of the code powering the MLAB deployment of DONAR.

Familiarity with PowerDNS and its PipeBackend interface is pre-requisite to understanding most of this code. See [this page](http://doc.powerdns.com/backends-detail.html#pipebackend) for information about the PowerDNS interchange format.

The following files will be most instructive to those hoping to learn about the code
`/donar/update/UpdateServer.java` (server which processes updates)
`/donar/update/client/UpdateClient.java` (client which sends updates)
`/donar/dns/DNSBackend.java` (PowerDNS protocol-compliant backend)
