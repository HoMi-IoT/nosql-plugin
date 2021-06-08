package org.homi.plugins.dbs.nosqlplugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.dizitart.no2.Filter;
import org.dizitart.no2.filters.Filters;
import org.homi.plugins.dbs.nosqlspec.query.AndQueryComponent;
import org.homi.plugins.dbs.nosqlspec.query.EqualityQueryComponent;
import org.homi.plugins.dbs.nosqlspec.query.IQueryComponent;
import org.homi.plugins.dbs.nosqlspec.query.IQueryVisitor;
import org.homi.plugins.dbs.nosqlspec.query.InQueryComponent;
import org.homi.plugins.dbs.nosqlspec.query.OrQueryComponent;

public class QueryVisitor implements IQueryVisitor<Filter> {
		
	@Override
	public Filter visit(OrQueryComponent qc) {
		List<Filter> filters = new ArrayList<>();
		for(IQueryComponent iqc: qc.getQueryComponents()){
			filters.add(iqc.accept(this));
		}
		return Filters.or((Filter[]) filters.toArray());
	}

	@Override
	public Filter visit(AndQueryComponent qc) {
		List<Filter> filters = new ArrayList<>();
		for(IQueryComponent iqc: qc.getQueryComponents()){
			filters.add(iqc.accept(this));
		}
		return Filters.and((Filter[]) filters.toArray());
	}

	@Override
	public Filter visit(EqualityQueryComponent qc) {
		return Filters.eq(qc.getKey(), qc.getValue());
	}

	@Override
	public <T extends Serializable & Comparable<T>> Filter visit(InQueryComponent<T> qc) {
		return Filters.in(qc.getKey(), (Object[])qc.getValues());
	}

}
