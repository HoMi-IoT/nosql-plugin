package org.homi.plugins.dbs.nosqlplugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.dizitart.no2.WriteResult;
import org.homi.plugin.api.commander.CommanderBuilder;
import org.homi.plugin.api.exceptions.InternalPluginException;
import org.homi.plugin.api.basicplugin.AbstractBasicPlugin;
import org.homi.plugins.dbs.nosqlspec.*;
import org.homi.plugins.dbs.nosqlspec.query.IQueryComponent;

import org.homi.plugins.dbs.nosqlspec.record.FieldList;
import org.homi.plugins.dbs.nosqlspec.record.IStorageComponent;
import org.homi.plugins.dbs.nosqlspec.record.Record;
import org.homi.plugins.dbs.nosqlspec.record.Value;

public class NoSQL extends AbstractBasicPlugin {
	private Nitrite database;

	@Override
	public void setup() {
		database = Nitrite.builder().openOrCreate();
		CommanderBuilder<NoSQLSpec> cb = new CommanderBuilder<>(NoSQLSpec.class);

		this.addCommander(NoSQLSpec.class,
				cb.onCommandEquals(NoSQLSpec.STORE, this::store)
					.onCommandEquals(NoSQLSpec.DELETE, this::delete)
					.onCommandEquals(NoSQLSpec.QUERY, this::query)
					.onCommandEquals(NoSQLSpec.UPDATE, this::update)
					.build());
	}
	
	@Override
	public void teardown() {
		database.close();
	}

	private Integer store(Object... params) throws InternalPluginException{
		try {
			NitriteCollection collection = database.getCollection((String) params[0]);
			Record record = (Record) params[1];
			DocumentConverter dc = new DocumentConverter();
			WriteResult r = collection.insert((Document)record.accept(dc));
			return r.getAffectedCount();
		} catch(Exception e) {
			throw new InternalPluginException(e);
		}
	}

	private Integer delete(Object... params) throws InternalPluginException{
		try {
			NitriteCollection collection = this.database.getCollection((String) params[0]);
			QueryVisitor qv = new QueryVisitor();
			IQueryComponent iqv = (IQueryComponent) params[1];
			var result = collection.remove(iqv.accept(qv));
			
			return result.getAffectedCount();
		} catch(Exception e) {
			throw new InternalPluginException(e);
		}
	}
	
	private Integer update(Object... params) throws InternalPluginException{
		try {
			NitriteCollection collection = this.database.getCollection((String) params[0]);
			QueryVisitor qv = new QueryVisitor();
			IQueryComponent iqv = (IQueryComponent) params[1];
			Record record = (Record) params[2];
			DocumentConverter dc = new DocumentConverter();
			WriteResult result;
			if(iqv == null)
				result = collection.update((Document)record.accept(dc));
			else
				result = collection.update(iqv.accept(qv), (Document)record.accept(dc));
			
			return result.getAffectedCount();
		} catch(Exception e) {
			throw new InternalPluginException(e);
		}
	}

	private FieldList query(Object... params) throws InternalPluginException{
		try {
			NitriteCollection collection = this.database.getCollection((String) params[0]);
			QueryVisitor qv = new QueryVisitor();
			IQueryComponent iqv = (IQueryComponent) params[1];
			Cursor result;
			if(iqv == null)
				result = collection.find();
			else
				result = collection.find(iqv.accept(qv));
			FieldList fl = new FieldList();
			result.forEach((doc)->{
				fl.addComponent(parseDocument(doc));
				});
			return fl;
		} catch(Exception e) {
			throw new InternalPluginException(e);
		}
	}

	private Record parseDocument(Document doc) {
		Record result = new Record();
		for(var entry :doc.entrySet()) {
			result.addField(entry.getKey(), buildComponent((Serializable) entry.getValue()) );
		}
		return result;
	}

	private FieldList parseList(List<?> list) {
		FieldList result = new FieldList();
		for(var element : list) {
			result.addComponent(buildComponent((Serializable) element));
		}
		return result;
	}
	
	private IStorageComponent buildComponent(Serializable value) {
		if(value instanceof Document) 
			return parseDocument((Document) value);
		if(value instanceof List) 
			return parseList((List<?>) value); 
		return new Value<>(value);
	}
}
