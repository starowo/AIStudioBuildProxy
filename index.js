// AI Studio Proxy Extension
// Entry point
import { proxies } from '../../../openai.js';
import { extension_settings, getContext, renderExtensionTemplateAsync } from '../../../extensions.js';

const extensionName = "AIStudioBuildProxy";
let satellite;
jQuery(async () => {
    try {
        const html = await renderExtensionTemplateAsync('third-party/AIStudioBuildProxy', 'index');
        $("#extensions_settings2").append(html);
        const SillyTavern = window.SillyTavern;
        if (!SillyTavern) {
            console.error(`${extensionName}: SillyTavern global object not found.`);
            return;
        }

        // check for proxy
        if (!proxies.find(proxy => proxy.name == "AIStudio反代")) {
            proxies.push({
                name: "AIStudio反代",
                url: "http://127.0.0.1:8889",
                password: ""
            });
            const option = document.createElement('option');
            option.text = String("AIStudio反代");
            option.value = String("AIStudio反代");

            $('#openai_proxy_preset').append(option);
        }

        const context = SillyTavern.getContext();
        console.log(`${extensionName} extension loaded.`);

        $("#aistudio_proxy_button").on("click", () => {
            if (satellite) {
                satellite.close();
            }
            satellite = window.open("https://ai.studio/apps/drive/1V2aNY3lZbtLZ58YQOfFnVg82uNikWqlM?fullscreenApplet=true", "satellite", "width=1280,height=720");
        });
    } catch (error) {
        console.error(`${extensionName} failed to load:`, error);
    }
});
