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

public class SgsConfig extends TemplateConfig {
	public SgsConfig() {
		addNamespaceAlias(-2, "Medėjė", "Medija");
		addNamespaceAlias(-1, "Specēlos", "Specialus", "Specialus");
		addNamespaceAlias(1, "Aptarėms", "Aptarimas", "Aptarimas");
		addNamespaceAlias(2, "Nauduotuos", "Naudotojas", "Naudotojas", "Naudotojas", "Naudotoja");
		addNamespaceAlias(3, "Nauduotuojė_aptarėms", "Naudotojo_aptarimas", "Naudotojo_aptarimas", "Naudotojo_aptarimas", "Naudotojos_aptarimas");
		addNamespaceAlias(5, "Wikipedia_aptarėms", "Wikipedia_aptarimas", "Wikipedia_aptarimas");
		addNamespaceAlias(6, "Abruozdielis", "Vaizdas", "Vaizdas");
		addNamespaceAlias(7, "Abruozdielė_aptarėms", "Vaizdo_aptarimas", "Vaizdo_aptarimas");
		addNamespaceAlias(8, "MediaWiki", "MediaWiki");
		addNamespaceAlias(9, "MediaWiki_aptarėms", "MediaWiki_aptarimas", "MediaWiki_aptarimas");
		addNamespaceAlias(10, "Šabluons", "Šablonas", "Šablonas");
		addNamespaceAlias(11, "Šabluona_aptarėms", "Šablono_aptarimas", "Šablono_aptarimas");
		addNamespaceAlias(12, "Pagelba", "Pagalba", "Pagalba");
		addNamespaceAlias(13, "Pagelbas_aptarėms", "Pagalbos_aptarimas", "Pagalbos_aptarimas");
		addNamespaceAlias(14, "Kateguorėjė", "Kategorija", "Kategorija");
		addNamespaceAlias(15, "Kateguorėjės_aptarėms", "Kategorijos_aptarimas", "Kategorijos_aptarimas");

		addI18nCIAlias("redirect", "#PERADRESAVIMAS", "#REDIRECT");
		addI18nCIAlias("notoc", "__BETURIN__", "__NOTOC__");
		addI18nCIAlias("nogallery", "__BEGALERIJOS__", "__NOGALLERY__");
		addI18nCIAlias("toc", "__TURINYS__", "__TOC__");
		addI18nCIAlias("noeditsection", "__BEREDAGSEKC__", "__NOEDITSECTION__");
		addI18nAlias("currentmonth", "DABARTINISMĖNESIS", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonthname", "DABARTINIOMĖNESIOPAVADINIMAS", "CURRENTMONTHNAME");
		addI18nAlias("currentday", "DABARTINĖDIENA", "CURRENTDAY");
		addI18nAlias("currentday2", "DABARTINĖDIENA2", "CURRENTDAY2");
		addI18nAlias("currentdayname", "DABARTINĖSDIENOSPAVADINIMAS", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "DABARTINIAIMETAI", "CURRENTYEAR");
		addI18nAlias("currenttime", "DABARTINISLAIKAS", "CURRENTTIME");
		addI18nAlias("currenthour", "DABARTINĖVALANDA", "CURRENTHOUR");
		addI18nAlias("numberofpages", "PUSLAPIŲSKAIČIUS", "NUMBEROFPAGES");
		addI18nAlias("numberofarticles", "STRAIPSNIŲSKAIČIUS", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "FAILŲSKAIČIUS", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "NAUDOTOJŲSKAIČIUS", "NUMBEROFUSERS");
		addI18nAlias("numberofedits", "KEITIMŲSKAIČIUS", "NUMBEROFEDITS");
		addI18nAlias("img_thumbnail", "miniatiūra", "mini", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "miniatiūra=$1", "mini=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_right", "dešinėje", "right");
		addI18nAlias("img_left", "kairėje", "left");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://sgs.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "sgs";
	}
}
