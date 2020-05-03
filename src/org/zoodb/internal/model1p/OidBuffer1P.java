/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal.model1p;

import org.zoodb.internal.OidBuffer;

public class OidBuffer1P extends OidBuffer {

	private final Node1P node;
	
	public OidBuffer1P(Node1P node) {
		this.node = node;
	}

	@Override
	public long[] allocateMoreOids() {
		return node.getDiskAccess().allocateOids(this.getOidAllocSize());
	}
	
}
