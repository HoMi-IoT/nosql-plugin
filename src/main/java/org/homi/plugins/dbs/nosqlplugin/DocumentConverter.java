package org.homi.plugins.dbs.nosqlplugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dizitart.no2.Document;
import org.homi.plugins.dbs.nosqlspec.record.FieldList;
import org.homi.plugins.dbs.nosqlspec.record.IStorageComponent;
import org.homi.plugins.dbs.nosqlspec.record.IStorageComponentVisitor;
import org.homi.plugins.dbs.nosqlspec.record.Record;
import org.homi.plugins.dbs.nosqlspec.record.Value;

public class DocumentConverter implements IStorageComponentVisitor<Object> {

	@Override
	public List<Object> visit(FieldList fieldList) {
		List<Object> result = new ArrayList<>();
		for (IStorageComponent comp : fieldList.getList()) {
			result.add(comp.accept(this));
		}
		return result;
	}

	@Override
	public Document visit(Record record) {
		Document result = new Document();
		for (Map.Entry<String, IStorageComponent> entry : record.getFields().entrySet()) {
			result.put(entry.getKey(), entry.getValue().accept(this));
		}
		return result;
	}

	@Override
	public <T extends Serializable> Object visit(Value<T> value) {
		return value.getValue();
	}

}
