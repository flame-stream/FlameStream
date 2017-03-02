/**
 * This file is part of Wikiforia.
 *
 * Wikiforia is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Wikiforia is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Wikiforia. If not, see <http://www.gnu.org/licenses/>.
 */
 package nlp.wikipedia.lang;

//Autogenerated from Wikimedia sources at 2015-04-16T13:55:11+00:00

public class GvConfig extends TemplateConfig {
	public GvConfig() {
		addNamespaceAlias(-2, "Meanyn");
		addNamespaceAlias(-1, "Er_lheh");
		addNamespaceAlias(1, "Resooney");
		addNamespaceAlias(2, "Ymmydeyr");
		addNamespaceAlias(3, "Resooney_ymmydeyr");
		addNamespaceAlias(5, "Resooney_Wikipedia");
		addNamespaceAlias(6, "Coadan");
		addNamespaceAlias(7, "Resooney_coadan");
		addNamespaceAlias(8, "MediaWiki");
		addNamespaceAlias(9, "Resooney_MediaWiki");
		addNamespaceAlias(10, "Clowan");
		addNamespaceAlias(11, "Resooney_clowan");
		addNamespaceAlias(12, "Cooney");
		addNamespaceAlias(13, "Resooney_cooney");
		addNamespaceAlias(14, "Ronney");
		addNamespaceAlias(15, "Resooney_ronney");

	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://gv.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "gv";
	}
}
