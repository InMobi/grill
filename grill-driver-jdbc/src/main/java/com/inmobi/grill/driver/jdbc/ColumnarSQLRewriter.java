package com.inmobi.grill.driver.jdbc;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FULLOUTERJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_JOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LEFTOUTERJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LEFTSEMIJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_RIGHTOUTERJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_UNIQUEJOIN;

import java.awt.LinearGradientPaint;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.CubeSemanticAnalyzer;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import com.inmobi.grill.api.GrillException;

public class ColumnarSQLRewriter implements QueryRewriter {
	private Configuration conf;
	private String clauseName = null;
	private QB qb;
	private ASTNode ast;
	private String query;
	private String finalFactQuery;
	private String limit;
	private StringBuilder factFilters = new StringBuilder();
	private StringBuilder factInLineQuery = new StringBuilder();

	protected StringBuilder allSubQueries = new StringBuilder();
	protected StringBuilder factKeys = new StringBuilder();
	protected StringBuilder rewrittenQuery = new StringBuilder();
	protected StringBuilder mergedQuery = new StringBuilder();
	protected String finalRewrittenQuery;

	protected StringBuilder joinCondition = new StringBuilder();
	protected List<String> allkeys = new ArrayList<String>();
	protected List<String> aggColumn = new ArrayList<String>();
	protected List<String> filterInJoinCond = new ArrayList<String>();
	protected List<String> rightFilter = new ArrayList<String>();

	private String leftFilter;
	private Map<String, String> mapAggTabAlias = new HashMap<String, String>();
	private static final Log LOG = LogFactory.getLog(ColumnarSQLRewriter.class);

	private String factTable;
	private String factAlias;
	private String whereTree;
	private String havingTree;
	private String orderByTree;
	private String selectTree;
	private String groupByTree;
	private String joinTree;

	private ASTNode joinAST;
	private ASTNode havingAST;
	private ASTNode selectAST;
	private ASTNode whereAST;
	private ASTNode orderByAST;
	private ASTNode groupByAST;

	public ColumnarSQLRewriter(HiveConf conf, String query) {
		// super(conf);
		this.conf = conf;
		this.query = query;
	}

	public ColumnarSQLRewriter() {
		// super(conf);
	}

	public String getClause() {
		if (clauseName == null) {
			TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo()
					.getClauseNames());
			clauseName = ks.first();
		}
		return clauseName;
	}

	// String query to ASTNode conversion
	public void getAST() {
		try {
			if (query.toLowerCase().matches("(.*)union all(.*)")) {
				String[] queries = query.toLowerCase().split("union all");
				for (int i = 0; i < queries.length; i++) {
					ast = HQLParser.parseHQL(queries[i]);
					System.out.println(ast);
				}
			} else {
				ast = HQLParser.parseHQL(query);
				HQLParser.printAST(ast);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	// Analyze query AST and split into sub trees
	public void analyzeInternal() throws SemanticException {
		HiveConf conf = new HiveConf();
		CubeSemanticAnalyzer c1 = new CubeSemanticAnalyzer(conf);

		QB qb = new QB(null, null, false);

		if (!c1.doPhase1(ast, qb, c1.initPhase1Ctx())) {
			return;
		}

		// Get clause name
		TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo()
				.getClauseNames());
		clauseName = ks.first();

		// Split query into trees
		if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
			this.whereTree = HQLParser.getString(qb.getParseInfo()
					.getWhrForClause(clauseName));
			this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
		}

		if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
			this.havingTree = HQLParser.getString(qb.getParseInfo()
					.getHavingForClause(clauseName));
			this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
		}

		if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
			this.orderByTree = HQLParser.getString(qb.getParseInfo()
					.getOrderByForClause(clauseName));
			this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
		}
		if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
			this.groupByTree = HQLParser.getString(qb.getParseInfo()
					.getGroupByForClause(clauseName));
			this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
		}

		if (qb.getParseInfo().getSelForClause(clauseName) != null) {
			this.selectTree = HQLParser.getString(qb.getParseInfo()
					.getSelForClause(clauseName));
			this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
		}

		if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
			this.joinTree = HQLParser
					.getString(qb.getParseInfo().getJoinExpr());
			this.joinAST = qb.getParseInfo().getJoinExpr();
		}

		getFactNameAlias();
	}

	// Get join conditions
	public void getJoinCond(ASTNode node) {

		if (node == null) {
			return;
		}
		int rootType = node.getToken().getType();
		if (rootType == TOK_JOIN || rootType == TOK_LEFTOUTERJOIN
				|| rootType == TOK_RIGHTOUTERJOIN
				|| rootType == TOK_FULLOUTERJOIN
				|| rootType == TOK_LEFTSEMIJOIN || rootType == TOK_UNIQUEJOIN) {

			ASTNode left = (ASTNode) node.getChild(0);
			ASTNode right = (ASTNode) node.getChild(1);

			String joinType = "";
			String JoinToken = node.getToken().getText();

			if (JoinToken.equals("TOK_JOIN")) {
				joinType = "inner join";
			} else if (JoinToken.equals("TOK_LEFTOUTERJOIN")) {
				joinType = "left outer join";
			} else if (JoinToken.equals("TOK_RIGHTOUTERJOIN")) {
				joinType = "right outer join";
			} else if (JoinToken.equals("TOK_FULLOUTERJOIN")) {
				joinType = "full outer join";
			} else if (JoinToken.equals("TOK_LEFTSEMIJOIN")) {
				joinType = "left semi join";
			} else if (JoinToken.equals("TOK_UNIQUEJOIN")) {
				joinType = "unique join";
			} else {
				LOG.info("Non supported join type : " + JoinToken);
			}
			String joinCond = "";

			if (node.getChildCount() > 2) {
				// User has specified a join condition for filter pushdown.
				joinCond = HQLParser.getString((ASTNode) node.getChild(2));
			}

			String joinClause = joinType.concat(HQLParser.getString(right))
					.concat(" on ").concat(joinCond);
			joinCondition.append(" ").append(joinClause);
		}
		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			getJoinCond(child);
		}
	}

	// Get filter conditions if user has specified a join
	// condition for filter pushdown.
	public void getFilterInJoinCond(ASTNode node) {

		if (node.getToken().getType() == HiveParser.KW_AND) {
			ASTNode right = (ASTNode) node.getChild(1);
			String filterCond = HQLParser.getString((ASTNode) right);
			rightFilter.add(filterCond);
		}

		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			getFilterInJoinCond(child);
		}
	}

	// match join tree with where tree to build subquery
	public void buildSubqueries(ASTNode node) {
		if (node == null) {
			LOG.debug("Join AST is null " + node);
			return;
		}

		String subquery = "";
		if (node.getToken().getType() == HiveParser.EQUAL) {
			if (node.getChild(0).getType() == HiveParser.DOT
					&& node.getChild(1).getType() == HiveParser.DOT) {

				ASTNode left = (ASTNode) node.getChild(0);
				ASTNode right = (ASTNode) node.getChild(1);

				String factJoinKeys = HQLParser.getString((ASTNode) left)
						.toString().replaceAll("\\s+", "")
						.replaceAll("[(,)]", "");
				String dimJoinKeys = HQLParser.getString((ASTNode) right)
						.toString().replaceAll("\\s+", "")
						.replaceAll("[(,)]", "");

				String dimTableName = dimJoinKeys.substring(0,
						dimJoinKeys.indexOf("."));
				factKeys.append(factJoinKeys).append(",");

				String queryphase1 = factJoinKeys.concat(" in ").concat(" ( ")
						.concat(" select ").concat(dimJoinKeys)
						.concat(" from ").concat(dimTableName)
						.concat(" where ");

				getAllFilters(whereAST);
				rightFilter.add(leftFilter);

				Set<String> setAllFilters = new HashSet<String>(rightFilter);

				// build other filters sub query
				if (setAllFilters.toString().matches(
						"(.*)".toString().concat(dimTableName).concat("(.*)"))
						|| setAllFilters.toString().matches(
								"(.*)".toString().concat(dimTableName)
										.concat("(.*)"))) {

					factFilters.delete(0, factFilters.length());

					// All filters in where clause
					for (int i = 0; i < setAllFilters.toArray().length; i++) {
						if (setAllFilters.toArray() != null) {
							if (setAllFilters.toArray()[i].toString().matches(
									"(.*)".toString().concat(dimTableName)
											.concat("(.*)"))) {
								String filters2 = setAllFilters.toArray()[i]
										.toString().concat(" and ");
								factFilters.append(filters2);
							}
						}
					}

					subquery = queryphase1.concat(
							factFilters.toString().substring(0,
									factFilters.toString().lastIndexOf("and")))
							.concat(")");
					allSubQueries.append(subquery).append(" and ");
				}
			}
		}
		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			buildSubqueries(child);
		}
	}

	// Get aggregate columns
	public ArrayList<String> getAggregateColumns(ASTNode node) {
		StringBuilder aggmeasures = new StringBuilder();
		if (HQLParser.isAggregateAST(node)) {
			if (node.getToken().getType() == HiveParser.TOK_FUNCTION
					|| node.getToken().getType() == HiveParser.DOT) {

				ASTNode right = (ASTNode) node.getChild(1);
				String aggCol = HQLParser.getString((ASTNode) right);

				String funident = HQLParser.findNodeByPath(node, Identifier)
						.toString();
				String measure = funident.concat("(").concat(aggCol)
						.concat(")");

				String alias = measure.replaceAll("\\s+", "")
						.replaceAll("\\(\\(", "_").replaceAll("[.]", "_")
						.replaceAll("[)]", "");
				String allaggmeasures = aggmeasures.append(measure)
						.append(" as ").append(alias).toString();
				String aggColAlias = funident.toString().concat("(")
						.concat(alias).concat(")");

				mapAggTabAlias.put(measure, aggColAlias);
				aggColumn.add(allaggmeasures);
				Set<String> setAggColumns = new HashSet<String>(aggColumn);
				aggColumn.clear();
				aggColumn.addAll(setAggColumns);
			}
		}

		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			getAggregateColumns(child);
		}
		return (ArrayList<String>) aggColumn;
	}

	// Get all columns in table.column format for fact table
	public ArrayList<String> getTablesAndColumns(ASTNode node) {

		if (node.getToken().getType() == HiveParser.DOT) {
			String table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
					Identifier).toString();
			String column = node.getChild(1).toString().toLowerCase();
			String keys = table.concat(".").concat(column);
			allkeys.add(keys);
		}
		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			getTablesAndColumns(child);
		}
		return (ArrayList<String>) allkeys;
	}

	// Get the limit value
	public String getLimitClause(ASTNode node) {

		if (node.getToken().getType() == HiveParser.TOK_LIMIT)
			limit = HQLParser.findNodeByPath(node, HiveParser.Number)
					.toString();

		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			getLimitClause(child);
		}
		return limit;
	}

	// Get all filters conditions in where clause
	public void getAllFilters(ASTNode node) {
		if (node == null) {
			return;
		}
		if (node.getToken().getType() == HiveParser.KW_AND) {
			ASTNode right = (ASTNode) node.getChild(1);
			String allFilters = HQLParser.getString((ASTNode) right);
			leftFilter = HQLParser.getString((ASTNode) node.getChild(0));
			rightFilter.add(allFilters);
		} else if (node.getToken().getType() == HiveParser.TOK_WHERE) {
			ASTNode right = (ASTNode) node.getChild(1);
			String allFilters = HQLParser.getString((ASTNode) right);
			leftFilter = HQLParser.getString((ASTNode) node.getChild(0));
			rightFilter.add(allFilters);
		}
		for (int i = 0; i < node.getChildCount(); i++) {
			ASTNode child = (ASTNode) node.getChild(i);
			getAllFilters(child);
		}

	}

	public void getFactNameAlias() {
		if (query.matches("(.*)where(.*)")) {
			String joinString = query.substring(query.indexOf("from"),
					query.indexOf("where"));
			String[] keys = joinString.split(" ");
			factTable = keys[1];
			factAlias = keys[2];

		} else if (query.matches("(.*)group by(.*)")) {
			String joinString = query.substring(query.indexOf("from"),
					query.indexOf("group by"));
			String[] keys = joinString.split(" ");
			factTable = keys[1];
			factAlias = keys[2];
			LOG.info("Input query missing where clause");
		} else if (query.matches("(.*)order by(.*)")) {
			String joinString = query.substring(query.indexOf("from"),
					query.indexOf("order by"));
			String[] keys = joinString.split(" ");
			factTable = keys[1];
			factAlias = keys[2];
			LOG.info("Input query missing where clause and group by clause");
		} else {
			LOG.info("Input query missing where, group by and order by clause");
		}
	}

	public void reset() {
		// rewrittenQuery.delete(0, rewrittenQuery.length());
		factInLineQuery.delete(0, factInLineQuery.length());
		factKeys.delete(0, factKeys.length());
		aggColumn.clear();
		factTable = "";
		factAlias = "";
		allSubQueries.delete(0, allSubQueries.length());
		rightFilter.clear();
		joinCondition.delete(0, joinCondition.length());
		// mergedQuery.delete(0, mergedQuery.length());
	}

	public void buildQuery() {

		try {
			analyzeInternal();
		} catch (SemanticException e) {
			e.printStackTrace();
		}

		getFilterInJoinCond(joinAST);
		getAggregateColumns(selectAST);
		getJoinCond(joinAST);
		getAllFilters(whereAST);
		buildSubqueries(joinAST);

		// Construct the final fact in-line query with keys,
		// measures and
		// individual sub queries built.

		if (whereTree == null || joinTree == null) {
			LOG.info("No filters specified in where clause. Nothing to rewrite");
			LOG.info("Input Query : " + query);
			LOG.info("Rewritten Query : " + query);
			finalRewrittenQuery = query;

		} else {
			factInLineQuery
					.append(" (select ")
					.append(factKeys)
					.append(aggColumn.toString().replace("[", "")
							.replace("]", "")).append(" from ")
					.append(factTable).append(" ").append(factAlias);
			if (allSubQueries != null)
				factInLineQuery.append(" where ");
			factInLineQuery.append(allSubQueries.toString().substring(0,
					allSubQueries.lastIndexOf("and")));
			factInLineQuery.append(" group by ");
			factInLineQuery.append(factKeys.toString().substring(0,
					factKeys.toString().lastIndexOf(",")));
			factInLineQuery.append(")");
		}

		// Replace the aggregate column aliases from fact
		// in-line query to the outer query

		for (Map.Entry<String, String> entry : mapAggTabAlias.entrySet()) {
			selectTree = selectTree.replace(entry.getKey(), entry.getValue());
			if (orderByTree != null) {
				orderByTree = orderByTree.replace(entry.getKey(),
						entry.getValue());
			}
			if (havingTree != null) {
				havingTree = havingTree.replace(entry.getKey(),
						entry.getValue());
			}
		}

		// Get the limit clause
		String limit = getLimitClause(ast);

		// Add fact table and alias to the join query
		String finalJoinClause = factTable.concat(" ").concat(factAlias)
				.concat(joinCondition.toString());

		// Construct the final rewritten query
		rewrittenQuery
				.append("select ")
				.append(selectTree)
				.append(" from ")
				.append(finalJoinClause.replaceAll(factTable,
						factInLineQuery.toString())).append(" where ")
				.append(whereTree).append(" group by ").append(groupByTree);
		if (havingTree != null)
			rewrittenQuery.append(" having ").append(havingTree);
		if (orderByTree != null)
			rewrittenQuery.append(" order by ").append(orderByTree);
		if (limit != null)

			rewrittenQuery.append(" limit ").append(limit);

	}

	@Override
	public String rewrite(Configuration conf, String query)
			throws GrillException {
		this.query = query;
		this.conf = conf;

		try {
			if (query.toLowerCase().matches("(.*)union all(.*)")) {
				String[] queries = query.toLowerCase().split("union all");
				for (int i = 0; i < queries.length; i++) {
					ast = HQLParser.parseHQL(queries[i]);
					buildQuery();
					mergedQuery = rewrittenQuery.append(" union all ");
					finalRewrittenQuery = mergedQuery.toString().substring(0,
							mergedQuery.lastIndexOf("union all"));

					LOG.info("Input Query : " + query);
					LOG.info("Rewritten Query :  " + finalRewrittenQuery);
					reset();
				}
			} else {
				ast = HQLParser.parseHQL(query);
				buildQuery();
				finalRewrittenQuery = rewrittenQuery.toString();
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return finalRewrittenQuery;
	}

}