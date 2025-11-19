// AI Studio Proxy Extension
// Entry point

const extensionName = "AIStudioBuildProxy";

jQuery(async () => {
    try {
        const SillyTavern = window.SillyTavern;
        if (!SillyTavern) {
            console.error(`${extensionName}: SillyTavern global object not found.`);
            return;
        }

        const context = SillyTavern.getContext();
        console.log(`${extensionName} extension loaded.`);

        // You can access context.chat, context.characters, etc. here
        // console.log("Current character:", context.characters[context.characterId]);

    } catch (error) {
        console.error(`${extensionName} failed to load:`, error);
    }
});
