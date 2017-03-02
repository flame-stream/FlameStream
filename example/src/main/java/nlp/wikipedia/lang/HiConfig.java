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

public class HiConfig extends TemplateConfig {
	public HiConfig() {
		addNamespaceAlias(-2, "मीडिया");
		addNamespaceAlias(-1, "विशेष");
		addNamespaceAlias(1, "वार्ता");
		addNamespaceAlias(2, "सदस्य");
		addNamespaceAlias(3, "सदस्य_वार्ता");
		addNamespaceAlias(5, "Wikipedia_वार्ता");
		addNamespaceAlias(6, "चित्र");
		addNamespaceAlias(7, "चित्र_वार्ता");
		addNamespaceAlias(8, "मीडियाविकि");
		addNamespaceAlias(9, "मीडियाविकि_वार्ता");
		addNamespaceAlias(10, "साँचा");
		addNamespaceAlias(11, "साँचा_वार्ता");
		addNamespaceAlias(12, "सहायता");
		addNamespaceAlias(13, "सहायता_वार्ता");
		addNamespaceAlias(14, "श्रेणी");
		addNamespaceAlias(15, "श्रेणी_वार्ता");

		addI18nCIAlias("redirect", "#अनुप्रेषित", "#REDIRECT");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://hi.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "hi";
	}
}
