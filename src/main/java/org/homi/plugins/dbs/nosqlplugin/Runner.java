package org.homi.plugins.dbs.nosqlplugin;

import java.util.ArrayList;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.homi.plugin.api.exceptions.InternalPluginException;
import org.homi.plugins.dbs.nosqlspec.query.QueryBuilder;
import org.homi.plugins.dbs.nosqlspec.record.FieldList;
import org.homi.plugins.dbs.nosqlspec.record.Record;
import org.homi.plugins.dbs.nosqlspec.record.Value;

public class Runner {
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
//		d.put("Hello", new String());
//		d.put("_id", 1233L);
//		nc.insert(d);
//		System.out.println(nc.find().firstOrDefault());
//		System.out.println((new ArrayList<>()).getClass().getModule().getName());
		
	
//		nc.getClass().getClassLoader().getPlatformClassLoader().
		NoSQL ns = new NoSQL();

		StorageComponentPrinter p = new StorageComponentPrinter();
		System.out.println(makeRecord().accept(p));
		
		
		
		ns.setup();
		try {
			System.out.println(ns.store("col1", makeRecord()));
//			System.out.println(ns.database.getCollection("col1").find().firstOrDefault());
			System.out.println(ns.store("col1", makeRecord()));
			System.out.println(ns.store("col1", makeRecord1()));
			System.out.println(ns.store("col1", makeRecord2()));

//			var rr = ns.query("col1", QueryBuilder.eq("k7", "126"));
//			var rr = ns.query("col1", null);
//			var rr = ns.query("col1", null);
//			System.out.println( rr.accept(p) );
//			System.out.println(ns.database.getCollection("col1").find().firstOrDefault());
			
//			System.out.println(ns.delete("col1", QueryBuilder.eq("k7", "126")));
			var rr = ns.query("col1", QueryBuilder.eq("k7", new String("126")));
			System.out.println( rr.accept(p) );
//			
//			System.out.println(ns.database.getCollection("col1").find().firstOrDefault());
		} catch (InternalPluginException e) {
			e.printStackTrace();
		}
	}

	private static Record makeRecord() {
		Record r = new Record();

		r.addField("key1", new Value<>(new String()));
		r.addField("key2", makeFieldList());
		r.addField("k7", new Value<>("126"));
		return r;
	}

	private static FieldList makeFieldList() {
		FieldList fl = new FieldList();
		fl.addComponent(new Value<>(Integer.valueOf(10)));
		fl.addComponent(makeRecord2());
		return fl;
	}

	private static Record makeRecord1() {
		Record r1 = new Record();

		r1.addField("key3", makeFieldList());
		return r1;
	}

	private static Record makeRecord2() {
		Record r2 = new Record();
		r2.addField("key4", new Value<>(new ArrayList<Object>()));
		return r2;
	}
}
