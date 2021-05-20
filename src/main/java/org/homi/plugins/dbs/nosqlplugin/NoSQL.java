package org.homi.plugins.dbs.nosqlplugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.dizitart.no2.*;

import org.homi.plugin.api.AbstractPlugin;
import org.homi.plugin.api.CommanderBuilder;
import org.homi.plugin.api.IPlugin;
import org.homi.plugin.specification.ISpecification;
import org.homi.plugins.dbs.nosqlspec.*;
import org.homi.plugins.dbs.nosqlspec.Record;
public class NoSQL extends AbstractPlugin {
	private Nitrite database;

	@Override
	public void setup() {
		database = Nitrite.builder().openOrCreate();
		// db.getCollection(null)
		CommanderBuilder<NoSQLSpec> cb = new CommanderBuilder<>(NoSQLSpec.class);

		this.addCommander(NoSQLSpec.class,
				cb.onCommandEquals(NoSQLSpec.STORE, this::store).onCommandEquals(NoSQLSpec.DELETE, this::delete)
						.onCommandEquals(NoSQLSpec.QUERY, this::query).onCommandEquals(NoSQLSpec.UPDATE, this::update)
						.build());

	}

	
	  public static void main(String[] args) {
	  
	  Nitrite db = Nitrite.builder().openOrCreate(); 
	  NitriteCollection nc = db.getCollection("collection"); 
	  
	  Record r = new Record();
	  Record r1 = new Record();
	  Record r2 = new Record();
	  r2.addField("key4", new Value(new ArrayList<Object>()));
	  FieldList fl = new FieldList();
	  fl.addComponent(new Value(10));
	  fl.addComponent(r2);
	  r1.addField("key3", fl);
	  r.addField("key1", new Value(new Custom()));
	  r.addField("key2", r1);
	  NoSQL ns = new NoSQL();
	  
	  System.out.println(r.printComponent(0));
	  nc.insert(ns.processRecord(r));
	  var curs = nc.find();
	  
	  curs.forEach((d)->{
		  System.out.println(d);
		  
	  });
	  
	 }
	 
	@Override
	public void teardown() {
		// TODO Auto-generated method stub

	}

	private Boolean store(Object... params) {
		NitriteCollection nc = database.getCollection((String)params[0]);
		Record r = (Record)params[1];
		
		
		nc.insert(processRecord(r));
		return true;
	}

	private Document processRecord(Record r) {
		Map<String, Component> recs = r.getValue();
		Document d = new Document();
		for(Map.Entry<String, Component> entry : recs.entrySet()) {
			if(entry.getValue().getType() == Value.class) {
				d.put(entry.getKey(), entry.getValue().getValue());
			}
			else if(entry.getValue().getType() == Record.class) {
				
				Document doc = processRecord((Record) entry.getValue());
				d.put(entry.getKey(), doc);
			}
			else if(entry.getValue().getType() == FieldList.class) {
				List<Object> fl = processFieldList((FieldList) entry.getValue());
				d.put(entry.getKey(), fl);
			}
		}
	
		return d;
	}
	
	


	private List<Object> processFieldList(FieldList value) {
		List<Object> c = new ArrayList<>();
		List<Component> fieldlist = value.getValue();
		for(Component comp : fieldlist) {
			if(comp.getType() == Value.class) {
				c.add(comp.getValue());
			}
			else if(comp.getType() == Record.class) {
				
				Document doc = processRecord((Record) comp);
				c.add(doc);
			}
			else if(comp.getType() == FieldList.class) {
				List<Object> fl = processFieldList(comp.getValue());
				c.add(fl);
			}
		}
		return c;
	}

	private Boolean delete(Object... params) {
		return true;
	}

	private Boolean update(Object... params) {
		return true;
	}

	private Boolean query(Object... params) {
		return true;
	}

}
