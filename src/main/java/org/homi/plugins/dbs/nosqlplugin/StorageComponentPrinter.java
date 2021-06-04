package org.homi.plugins.dbs.nosqlplugin;

import java.io.Serializable;
import java.util.Map;

import org.homi.plugins.dbs.nosqlspec.record.FieldList;
import org.homi.plugins.dbs.nosqlspec.record.IStorageComponent;
import org.homi.plugins.dbs.nosqlspec.record.IStorageComponentVisitor;
import org.homi.plugins.dbs.nosqlspec.record.Record;
import org.homi.plugins.dbs.nosqlspec.record.Value;

public class StorageComponentPrinter implements IStorageComponentVisitor<String>{

	private int level;
	@Override
	public String visit(FieldList fieldList) {
		StringBuilder sb = new StringBuilder();
		sb.append("[\n");
		this.level++;
		for(IStorageComponent element : fieldList.getList()) {
			sb.append(this.indent(level*4));
			sb.append(element.accept(this)+",");
			sb.append("\n");
		}
		this.level--;
		sb.append(this.indent(level*4)+"]");
		return sb.toString();
	}

	@Override
	public String visit(Record record) {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		this.level++;
		for(Map.Entry<String, IStorageComponent> entry : record.getFields().entrySet()) {
			sb.append(this.indent(level*4));
			sb.append(entry.getKey() + ": " + entry.getValue().accept(this)+",");
			sb.append("\n");
		}
		this.level--;
		sb.append(this.indent(level*4)+"}");
		return sb.toString();
	}

	@Override
	public <T extends Serializable> String visit(Value<T> value) {
		return value.getValue().toString();
	}

	private String indent(int indent) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0 ; i < indent; i++) {
			sb.append(" ");
		}
		return sb.toString();
	}
}
