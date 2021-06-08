module nosqlplugin {
	requires org.homi.plugin.api;
	requires nosqlspec;
	requires nitrite;
	requires org.homi.plugin.specification;
	provides org.homi.plugin.api.basicplugin.IBasicPlugin
	with org.homi.plugins.dbs.nosqlplugin.NoSQL;
}