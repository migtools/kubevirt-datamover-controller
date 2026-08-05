/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import "strconv"

// ParseBool parses a boolean string using strconv.ParseBool, which accepts
// 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False.
// Returns false for empty or unparseable strings.
func ParseBool(s string) bool {
	v, _ := strconv.ParseBool(s)
	return v
}
