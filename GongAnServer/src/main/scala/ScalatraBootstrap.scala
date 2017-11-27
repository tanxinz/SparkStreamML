import com.sibat.gongan._
import org.scalatra._
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new PersonalAction, "/api/personal")
    context.mount(new MACAction, "/api/mac")
    context.mount(new ODSAction, "/api/ods")
    context.mount(new IDNOAction, "/api/idno")
    context.mount(new IMSIAction, "/api/imsi")
    context.mount(new TYMACAction, "/api/ty_mac")
    context.mount(new TYIMSIAction, "/api/ty_imsi")
    context.mount(new SZTAction, "/api/szt")
    context.mount(new APPointAction, "/api/ap_point")
    context.mount(new WarnningAction, "/api/warnning")
    context.mount(new APHeatMapAction, "/api/ap_heatmap")
    context.mount(new RZXFeatureAction, "/api/rzx_feature")
    context.mount(new SensordoorIdcardAction, "/api/sensordoor_idcard")

    context.mount(new ZaojiaAction, "/api/zaojia")
  }
}
