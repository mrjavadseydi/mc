#!/bin/bash
#
# Copyright (c) 2015-2021 MinIO, Inc.
#
# This file is part of MinIO Object Storage stack
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

cd "$(dirname "${BASH_SOURCE[0]}")"

# This historical export covers Git authors only. Print to stdout so it cannot
# overwrite CONTRIBUTORS.md, which also recognizes issue and unmerged PR authors.
# See .mailmap for how email addresses and names are deduplicated.

{
	cat <<-'EOH'
## Git commit authors

This export includes inherited upstream history. See CONTRIBUTORS.md for the
current SILO community record, including issue and pull-request authors.
	EOH
	echo
	git log --format='%aN <%aE>' | LC_ALL=C.UTF-8 sort -uf | sed 's/^/- /g'
}
