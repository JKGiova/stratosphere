# Historical development reader fixtures

`campaigns.tar.gz` contains authored v1 and v2 campaigns captured with the pre-removal producer.
`ids.json` names their campaign identities. The archive includes a synthetic registry, sealed source
brief, frozen datasets, application-compiled programs, fake worker results and hash-chained receipts.
All model replies and market data were authored fixtures; no provider, broker or real credentials
were used. This is compatibility evidence, not provider rights, profitability or execution authority.

Stored display paths refer to the original temporary fixture directory. They remain sealed bytes;
readers locate the actual artifacts under the supplied store root and never execute those paths.
Tests extract the archive into a private temporary directory and check that reads do not rewrite it.
