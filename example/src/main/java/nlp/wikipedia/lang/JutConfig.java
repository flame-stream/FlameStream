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

public class JutConfig extends TemplateConfig {
	public JutConfig() {
		addNamespaceAlias(-2, "Media");
		addNamespaceAlias(-1, "Speciel");
		addNamespaceAlias(1, "Diskussion");
		addNamespaceAlias(2, "Bruger");
		addNamespaceAlias(3, "Brugerdiskussion");
		addNamespaceAlias(5, "Wikipedia_diskussion", "Wikipedia-diskussion");
		addNamespaceAlias(6, "Fil", "Billede");
		addNamespaceAlias(7, "Fildiskussion", "Billeddiskussion");
		addNamespaceAlias(8, "MediaWiki");
		addNamespaceAlias(9, "MediaWiki_diskussion", "MediaWiki-diskussion");
		addNamespaceAlias(10, "Skabelon");
		addNamespaceAlias(11, "Skabelondiskussion");
		addNamespaceAlias(12, "Hjælp");
		addNamespaceAlias(13, "Hjælp_diskussion", "Hjælp-diskussion");
		addNamespaceAlias(14, "Kategori");
		addNamespaceAlias(15, "Kategoridiskussion");

	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://jut.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "jut";
	}
}
