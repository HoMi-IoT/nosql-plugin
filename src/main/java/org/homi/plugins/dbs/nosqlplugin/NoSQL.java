package org.homi.plugins.dbs.nosqlplugin;

import java.util.ArrayList;
import java.util.List;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;

import org.homi.plugin.api.commander.CommanderBuilder;
import org.homi.plugin.api.exceptions.InternalPluginException;
import org.homi.plugin.api.basicplugin.AbstractBasicPlugin;
import org.homi.plugins.dbs.nosqlspec.*;
import org.homi.plugins.dbs.nosqlspec.query.IQueryComponent;
import static org.homi.plugins.dbs.nosqlspec.query.QueryBuilder.*;
import org.homi.plugins.dbs.nosqlspec.record.FieldList;
import org.homi.plugins.dbs.nosqlspec.record.Record;
import org.homi.plugins.dbs.nosqlspec.record.Value;

public class NoSQL extends AbstractBasicPlugin {
	private Nitrite database;

	@Override
	public void setup() {
		database = Nitrite.builder().openOrCreate();
		// db.getCollection(null)
		CommanderBuilder<NoSQLSpec> cb = new CommanderBuilder<>(NoSQLSpec.class);

		this.addCommander(NoSQLSpec.class,
				cb.onCommandEquals(NoSQLSpec.STORE, this::store)
					.onCommandEquals(NoSQLSpec.DELETE, this::delete)
					.onCommandEquals(NoSQLSpec.QUERY, this::query)
//					.onCommandEquals(NoSQLSpec.UPDATE, this::update)
					.build());
		
		IQueryComponent query = or(eq("k1", 11), 
									and(
										eq("k1", 12),
										eq("k2", 17)));
//		q("coolection1", query);
	}

	public static void main(String[] args) {
//		Object o = new Custom();
//		
//		if(null instanceof Serializable) {
//			System.out.println("It's serializable");
//		}
//		Serializable s = (Serializable)o;
		Nitrite db = Nitrite.builder().openOrCreate();
		NitriteCollection nc = db.getCollection("collection");
//
//		Document d = new Document();
//		d.put("Hello", new Object());
//		nc.insert(d);
//		nc.getClass().getClassLoader().getPlatformClassLoader().
		Record r = new Record();
		Record r1 = new Record();
		Record r2 = new Record();

		r2.addField("key4", new Value<>(new ArrayList<Object>()));
		FieldList fl = new FieldList();
		fl.addComponent(new Value<>(Integer.valueOf(10)));
		fl.addComponent(r2);
		r1.addField("key3", fl);
		r.addField("key1", new Value<>(new Custom()));
		r.addField("key2", r1);
		r.addField("k7", new Value<>("126"));
		NoSQL ns = new NoSQL();

		StorageComponentPrinter p = new StorageComponentPrinter();
		System.out.println(r.accept(p));
	}
	
	@Override
	public void teardown() {
		database.close();

	}

	private Integer store(Object... params) throws InternalPluginException{
		try {
			NitriteCollection nc = database.getCollection((String) params[0]);
			Record record = (Record) params[1];
			DocumentConverter dc = new DocumentConverter();
			var r = nc.insert((Document[]) record.accept(dc));
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

	private Object query(Object... params) throws InternalPluginException{
		try {
			NitriteCollection collection = this.database.getCollection((String) params[0]);
			QueryVisitor qv = new QueryVisitor();
			IQueryComponent iqv = (IQueryComponent) params[1];
			
			var result = collection.find(iqv.accept(qv));
			return null; // convert result to a return value;
		} catch(Exception e) {
			throw new InternalPluginException(e);
		}
	}

}
