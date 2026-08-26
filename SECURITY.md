<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security policy

Please report suspected vulnerabilities privately through GitHub Security
Advisories for this repository. Do not include credentials, connection strings,
customer data, SQL parameters, or production topology in a public issue.

Supported releases receive security fixes on the latest `1.x` release line.
The adapter deliberately redacts SQL/driver failures before they cross the
Meridian SPI and never places resolved secrets in manifests or evidence.
