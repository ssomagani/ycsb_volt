/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * This client provideds a wrapper layer for running the Yahoo Cloud Serving
 * Benchmark (YCSB) against VoltDB. This benchmark runs a synchronous client
 * with a mix of the operations provided below. YCSB is open-source, and may
 * be found at https://github.com/brianfrankcooper/YCSB. The YCSB jar must be
 * in your classpath to compile this client.
 */
package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.voltdb.VoltTable;
import org.voltdb.client.AllPartitionProcedureCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class VoltClient4 extends DB {
	private Client m_client;
	private byte[] m_workingData;
	private ByteBuffer m_writeBuf;

	private static final Charset UTF8 = Charset.forName("UTF-8");

	@Override
	public void init() throws DBException {
		Properties props = getProperties();
		String servers = props.getProperty("voltdb.servers", "localhost");
		String user = props.getProperty("voltdb.user", "");
		String password = props.getProperty("voltdb.password", "");
		String strLimit = props.getProperty("voltdb.ratelimit");
		int ratelimit = strLimit != null ? Integer.parseInt(strLimit) : Integer.MAX_VALUE;
		try {
			m_client = ConnectionHelper.createConnection(Thread.currentThread().getId(), servers, user, password,
					ratelimit);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBException(e.getMessage());
		}
		m_workingData = new byte[1024 * 1024];
		m_writeBuf = ByteBuffer.wrap(m_workingData);
	}

	@Override
	public void cleanup() throws DBException {
		ConnectionHelper.disconnect(Thread.currentThread().getId());
	}

	@Override
	public Status delete(String keyspace, String key) {
		try {
			ClientResponse response = m_client.callProcedure("STORE.delete", keyspace.getBytes(UTF8), key);
			return response.getStatus() == ClientResponse.SUCCESS ? Status.OK : Status.ERROR;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	@Override
	public Status insert(String keyspace, String key, HashMap<String, ByteIterator> columns) {
		return update(keyspace, key, columns);
	}

	@Override
	public Status read(String keyspace, String key, Set<String> columns, HashMap<String, ByteIterator> result) {
		try {
			ClientResponse response = m_client.callProcedure("Get", keyspace.getBytes(UTF8), key);
			if (response.getStatus() != ClientResponse.SUCCESS) {
				return Status.ERROR;
			}
			VoltTable table = response.getResults()[0];
			if (table.advanceRow()) {
				unpackRowData(table, columns, result);
			}
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	@Override
	public Status scan(String keyspace, String lowerBound, int recordCount, Set<String> columns,
			Vector<HashMap<String, ByteIterator>> result) {
		try {
			ScanCallback callback = new ScanCallback(recordCount);
			m_client.callAllPartitionProcedure(callback, "Scan", keyspace, lowerBound, recordCount);
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	@Override
	public Status update(String keyspace, String key, HashMap<String, ByteIterator> columns) {
		try {
			ClientResponse response = m_client.callProcedure("Put", keyspace.getBytes(UTF8), key, packRowData(columns));
			return response.getStatus() == ClientResponse.SUCCESS ? Status.OK : Status.ERROR;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	private static class ScanCallback implements AllPartitionProcedureCallback {
		
		final int recordCount;
		public ScanCallback(int recordCount) {
			this.recordCount = recordCount;
		}

		@Override
		public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
			/* 
			int totalMatchesFound = 0;
			for (int i = 0; i < responses.length; i++) {
				ClientResponse response = responses[i].response;
				VoltTable[] results = response.getResults();
				if (results.length > 0) {
					VoltTable result = results[0];
					int rowCount = result.getRowCount();
					totalMatchesFound += rowCount;
				}
			}
			System.out.println("Found a total of " + totalMatchesFound + " results");
			*/
		}
	}

	private byte[] packRowData(HashMap<String, ByteIterator> columns) {
		m_writeBuf.clear();
		m_writeBuf.putInt(columns.size());
		for (String key : columns.keySet()) {
			byte[] k = key.getBytes(UTF8);
			m_writeBuf.putInt(k.length);
			m_writeBuf.put(k);

			ByteIterator v = columns.get(key);
			int len = (int) v.bytesLeft();
			m_writeBuf.putInt(len);
			v.nextBuf(m_workingData, m_writeBuf.position());
			m_writeBuf.position(m_writeBuf.position() + len);
		}

		byte[] data = new byte[m_writeBuf.position()];
		System.arraycopy(m_workingData, 0, data, 0, data.length);
		return data;
	}

	private HashMap<String, ByteIterator> unpackRowData(VoltTable data, Set<String> fields) {
		byte[] rowData = data.getVarbinary(0);
		ByteBuffer buf = ByteBuffer.wrap(rowData);
		int nFields = buf.getInt();
		int size = fields != null ? Math.min(fields.size(), nFields) : nFields;
		HashMap<String, ByteIterator> res = new HashMap<String, ByteIterator>(size, (float) 1.25);
		return unpackRowData(rowData, buf, nFields, fields, res);
	}

	private HashMap<String, ByteIterator> unpackRowData(VoltTable data, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		byte[] rowData = data.getVarbinary(0);
		ByteBuffer buf = ByteBuffer.wrap(rowData);
		int nFields = buf.getInt();
		return unpackRowData(rowData, buf, nFields, fields, result);
	}

	private HashMap<String, ByteIterator> unpackRowData(byte[] rowData, ByteBuffer buf, int nFields, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		for (int i = 0; i < nFields; i++) {
			int len = buf.getInt();
			int off = buf.position();
			String key = new String(rowData, off, len, UTF8);
			buf.position(off + len);
			len = buf.getInt();
			off = buf.position();
			if (fields == null || fields.contains(key)) {
				result.put(key, new ByteArrayByteIterator(rowData, off, len));
			}
			buf.position(off + len);
		}
		return result;
	}
}
