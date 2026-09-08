# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Load only the checked-in example; never alter package resolution paths."""

import importlib.util
from pathlib import Path

path = Path(__file__).resolve().parents[2] / "examples/versioned_target.py"
spec = importlib.util.spec_from_file_location("versioned_target_example", path)
assert spec is not None and spec.loader is not None
composition = importlib.util.module_from_spec(spec)
spec.loader.exec_module(composition)
