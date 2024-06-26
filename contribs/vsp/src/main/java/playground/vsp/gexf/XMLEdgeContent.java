//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vhudson-jaxb-ri-2.1-558 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.09.19 at 03:18:45 PM MESZ 
//


package playground.vsp.gexf;

import java.util.ArrayList;
import java.util.List;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlSchemaType;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.adapters.CollapsedStringAdapter;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import playground.vsp.gexf.viz.ColorContent;
import playground.vsp.gexf.viz.EdgeShapeContent;
import playground.vsp.gexf.viz.ThicknessContent;


/**
 * <p>Java class for edge-content complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="edge-content">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;choice maxOccurs="unbounded" minOccurs="0">
 *         &lt;element ref="{http://www.gexf.net/1.2draft}attvalues"/>
 *         &lt;element ref="{http://www.gexf.net/1.2draft}spells"/>
 *         &lt;choice>
 *           &lt;element ref="{http://www.gexf.net/1.2draft}color"/>
 *           &lt;element ref="{http://www.gexf.net/1.2draft}thickness"/>
 *           &lt;element name="shape" type="{http://www.gexf.net/1.2draft/viz}edge-shape-content"/>
 *         &lt;/choice>
 *       &lt;/choice>
 *       &lt;attribute name="start" type="{http://www.gexf.net/1.2draft}time-type" />
 *       &lt;attribute name="startopen" type="{http://www.gexf.net/1.2draft}time-type" />
 *       &lt;attribute name="end" type="{http://www.gexf.net/1.2draft}time-type" />
 *       &lt;attribute name="endopen" type="{http://www.gexf.net/1.2draft}time-type" />
 *       &lt;attribute name="id" use="required" type="{http://www.gexf.net/1.2draft}id-type" />
 *       &lt;attribute name="type" type="{http://www.gexf.net/1.2draft}edgetype-type" />
 *       &lt;attribute name="label" type="{http://www.w3.org/2001/XMLSchema}token" />
 *       &lt;attribute name="source" use="required" type="{http://www.gexf.net/1.2draft}id-type" />
 *       &lt;attribute name="target" use="required" type="{http://www.gexf.net/1.2draft}id-type" />
 *       &lt;attribute name="weight" type="{http://www.gexf.net/1.2draft}weight-type" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "edge-content", propOrder = {
    "attvaluesOrSpellsOrColor"
})
public class XMLEdgeContent {

    @XmlElements({
        @XmlElement(name = "color", type = ColorContent.class),
        @XmlElement(name = "attvalues", type = XMLAttvaluesContent.class),
        @XmlElement(name = "shape", type = EdgeShapeContent.class),
        @XmlElement(name = "spells", type = XMLSpellsContent.class),
        @XmlElement(name = "thickness", type = ThicknessContent.class)
    })
    protected List<Object> attvaluesOrSpellsOrColor;
    @XmlAttribute
    protected String start;
    @XmlAttribute
    protected String startopen;
    @XmlAttribute
    protected String end;
    @XmlAttribute
    protected String endopen;
    @XmlAttribute(required = true)
    protected String id;
    @XmlAttribute
    protected XMLEdgetypeType type;
    @XmlAttribute
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "token")
    protected String label;
    @XmlAttribute(required = true)
    protected String source;
    @XmlAttribute(required = true)
    protected String target;
    @XmlAttribute
    protected Float weight;

    /**
     * Gets the value of the attvaluesOrSpellsOrColor property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the attvaluesOrSpellsOrColor property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getAttvaluesOrSpellsOrColor().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ColorContent }
     * {@link XMLAttvaluesContent }
     * {@link EdgeShapeContent }
     * {@link XMLSpellsContent }
     * {@link ThicknessContent }
     * 
     * 
     */
    public List<Object> getAttvaluesOrSpellsOrColor() {
        if (attvaluesOrSpellsOrColor == null) {
            attvaluesOrSpellsOrColor = new ArrayList<Object>();
        }
        return this.attvaluesOrSpellsOrColor;
    }

    /**
     * Gets the value of the start property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getStart() {
        return start;
    }

    /**
     * Sets the value of the start property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setStart(String value) {
        this.start = value;
    }

    /**
     * Gets the value of the startopen property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getStartopen() {
        return startopen;
    }

    /**
     * Sets the value of the startopen property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setStartopen(String value) {
        this.startopen = value;
    }

    /**
     * Gets the value of the end property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getEnd() {
        return end;
    }

    /**
     * Sets the value of the end property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setEnd(String value) {
        this.end = value;
    }

    /**
     * Gets the value of the endopen property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getEndopen() {
        return endopen;
    }

    /**
     * Sets the value of the endopen property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setEndopen(String value) {
        this.endopen = value;
    }

    /**
     * Gets the value of the id property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the value of the id property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setId(String value) {
        this.id = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link XMLEdgetypeType }
     *     
     */
    public XMLEdgetypeType getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link XMLEdgetypeType }
     *     
     */
    public void setType(XMLEdgetypeType value) {
        this.type = value;
    }

    /**
     * Gets the value of the label property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLabel() {
        return label;
    }

    /**
     * Sets the value of the label property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLabel(String value) {
        this.label = value;
    }

    /**
     * Gets the value of the source property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSource() {
        return source;
    }

    /**
     * Sets the value of the source property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSource(String value) {
        this.source = value;
    }

    /**
     * Gets the value of the target property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTarget() {
        return target;
    }

    /**
     * Sets the value of the target property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTarget(String value) {
        this.target = value;
    }

    /**
     * Gets the value of the weight property.
     * 
     * @return
     *     possible object is
     *     {@link Float }
     *     
     */
    public Float getWeight() {
        return weight;
    }

    /**
     * Sets the value of the weight property.
     * 
     * @param value
     *     allowed object is
     *     {@link Float }
     *     
     */
    public void setWeight(Float value) {
        this.weight = value;
    }

}
