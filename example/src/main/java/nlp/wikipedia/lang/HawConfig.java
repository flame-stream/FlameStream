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

public class HawConfig extends TemplateConfig {
	public HawConfig() {
		addNamespaceAlias(-2, "Pāpaho");
		addNamespaceAlias(-1, "Papa_nui");
		addNamespaceAlias(1, "Kūkākūkā");
		addNamespaceAlias(2, "Mea_hoʻohana");
		addNamespaceAlias(3, "Kūkākūkā_o_mea_hoʻohana");
		addNamespaceAlias(5, "Kūkākūkā_o_Wikipikia");
		addNamespaceAlias(6, "Waihona", "Kiʻi");
		addNamespaceAlias(7, "Kūkākūkā_o_waihona", "Kūkākūkā_o_kiʻi");
		addNamespaceAlias(8, "MediaWiki");
		addNamespaceAlias(9, "Kūkākūkā_o_MediaWiki");
		addNamespaceAlias(10, "Anakuhi");
		addNamespaceAlias(11, "Kūkākūkā_o_anakuhi");
		addNamespaceAlias(12, "Kōkua");
		addNamespaceAlias(13, "Kūkākūkā_o_kōkua");
		addNamespaceAlias(14, "Māhele");
		addNamespaceAlias(15, "Kūkākūkā_o_māhele");

		addI18nAlias("currentmonth", "KĒIAMAHINA", "KEIAMAHINA", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonthname", "KĒIAINOAMAHINA", "KEIAINOAMAHINA", "CURRENTMONTHNAME");
		addI18nAlias("currentday", "KĒIALĀ", "KEIALA", "CURRENTDAY");
		addI18nAlias("currentday2", "KĒIALĀ2", "KEIALA2", "CURRENTDAY2");
		addI18nAlias("currentdayname", "KĒIAINOALĀ", "KEIAINOALA", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "KĒIAMAKAHIKI", "KEIAMAKAHIKI", "CURRENTYEAR");
		addI18nAlias("currenttime", "KĒIAMANAWA", "KEIAMANAWA", "CURRENTTIME");
		addI18nAlias("currenthour", "KĒIAHOLA", "KEIAHOLA", "CURRENTHOUR");
		addI18nAlias("numberofpages", "HELUʻAOʻAO", "HELUAOAO", "NUMBEROFPAGES");
		addI18nAlias("numberofarticles", "HELUMEA", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "HELUWAIHONA", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "HELUMEAHOʻOHANA", "HELUMEAHOOHANA", "NUMBEROFUSERS");
		addI18nAlias("numberofedits", "HELULOLI", "NUMBEROFEDITS");
		addI18nAlias("pagename", "INOAʻAOʻAO", "INOAAOAO", "PAGENAME");
		addI18nAlias("img_right", "ʻākau", "ākau", "akau", "right");
		addI18nAlias("img_left", "hema", "left");
		addI18nAlias("img_none", "ʻaʻohe", "aohe", "none");
		addI18nAlias("img_link", "loulou=$1", "link=$1");
		addI18nAlias("currentweek", "KĒIAPULE", "KEIAPULE", "CURRENTWEEK");
		addI18nCIAlias("language", "#ʻŌLELO", "#ŌLELO", "#OLELO", "#LANGUAGE:");
		addI18nAlias("numberofadmins", "HELUKAHU", "NUMBEROFADMINS");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://haw.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "haw";
	}
}
