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

public class TkConfig extends TemplateConfig {
	public TkConfig() {
		addNamespaceAlias(-2, "Media");
		addNamespaceAlias(-1, "Ýörite");
		addNamespaceAlias(1, "Çekişme");
		addNamespaceAlias(2, "Ulanyjy");
		addNamespaceAlias(3, "Ulanyjy_çekişme");
		addNamespaceAlias(5, "Wikipedia_çekişme");
		addNamespaceAlias(6, "Faýl");
		addNamespaceAlias(7, "Faýl_çekişme");
		addNamespaceAlias(9, "MediaWiki_çekişme");
		addNamespaceAlias(10, "Şablon");
		addNamespaceAlias(11, "Şablon_çekişme");
		addNamespaceAlias(12, "Ýardam");
		addNamespaceAlias(13, "Ýardam_çekişme");
		addNamespaceAlias(14, "Kategoriýa");
		addNamespaceAlias(15, "Kategoriýa_çekişme");

	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://tk.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "tk";
	}
}
