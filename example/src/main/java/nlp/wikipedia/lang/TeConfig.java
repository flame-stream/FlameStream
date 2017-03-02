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

public class TeConfig extends TemplateConfig {
	public TeConfig() {
		addNamespaceAlias(-2, "మీడియా");
		addNamespaceAlias(-1, "ప్రత్యేక");
		addNamespaceAlias(1, "చర్చ");
		addNamespaceAlias(2, "వాడుకరి", "సభ్యులు", "సభ్యుడు");
		addNamespaceAlias(3, "వాడుకరి_చర్చ", "సభ్యులపై_చర్చ", "సభ్యునిపై_చర్చ");
		addNamespaceAlias(5, "Wikipedia_చర్చ");
		addNamespaceAlias(6, "దస్త్రం", "బొమ్మ", "ఫైలు");
		addNamespaceAlias(7, "దస్త్రంపై_చర్చ", "బొమ్మపై_చర్చ", "ఫైలుపై_చర్చ");
		addNamespaceAlias(8, "మీడియావికీ");
		addNamespaceAlias(9, "మీడియావికీ_చర్చ");
		addNamespaceAlias(10, "మూస");
		addNamespaceAlias(11, "మూస_చర్చ");
		addNamespaceAlias(12, "సహాయం", "సహాయము");
		addNamespaceAlias(13, "సహాయం_చర్చ", "సహాయము_చర్చ");
		addNamespaceAlias(14, "వర్గం");
		addNamespaceAlias(15, "వర్గం_చర్చ");

		addI18nCIAlias("redirect", "#దారిమార్పు", "#REDIRECT");
		addI18nCIAlias("notoc", "__విషయసూచికవద్దు__", "__NOTOC__");
		addI18nCIAlias("toc", "__విషయసూచిక__", "__TOC__");
		addI18nAlias("pagename", "పేజీపేరు", "PAGENAME");
		addI18nAlias("img_right", "కుడి", "right");
		addI18nAlias("img_left", "ఎడమ", "left");
		addI18nCIAlias("special", "ప్రత్యేక", "special");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://te.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "te";
	}
}
