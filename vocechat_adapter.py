# vocechat_adapter.py
import asyncio
import json
from typing import Dict, Any, Optional
from urllib.parse import quote_plus 
import uuid 
import base64 
import mimetypes 
import os

import aiohttp
from aiohttp import web

from astrbot.api.platform import Platform, AstrBotMessage, MessageMember, PlatformMetadata, MessageType
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain, Image
from astrbot.core.platform.astr_message_event import MessageSesion 
from astrbot.api.platform import register_platform_adapter
from astrbot import logger

from .vocechat_event import VoceChatEvent 

DEFAULT_CONFIG_TMPL = {
    "vocechat_server_url": "http://localhost:3009", 
    "api_key": "YOUR_VOCECHAT_BOT_API_KEY",        
    "webhook_path": "/vocechat_webhook",           
    "webhook_listen_host": "0.0.0.0",              
    "webhook_port": 8080,                          
    "get_user_nickname_from_api": True, 
    "send_plain_as_markdown": False,     
    "default_bot_self_uid": "YOUR_BOT_USER_ID_IN_VOCECHAT" 
}

@register_platform_adapter("vocechat", "VoceChat 适配器", default_config_tmpl=DEFAULT_CONFIG_TMPL)
class VoceChatAdapter(Platform):

    def __init__(self, platform_config: dict, platform_settings: dict, event_queue: asyncio.Queue) -> None:
        super().__init__(platform_settings,event_queue)
        self.config = platform_config
        self.settings = platform_settings
        self.server_url = self.config.get("vocechat_server_url", "").rstrip('/')
        self.api_key = self.config.get("api_key", "")
        self.webhook_path = self.config.get("webhook_path", "/vocechat_webhook")
        self.listen_host = self.config.get("webhook_listen_host", "0.0.0.0")
        self.listen_port = int(self.config.get("webhook_port", 8080))
        self.get_user_nickname_from_api = self.config.get("get_user_nickname_from_api", True)
        self.send_plain_as_markdown = self.config.get("send_plain_as_markdown", False)
        self.default_bot_self_uid = str(self.config.get("default_bot_self_uid", "0")) 

        platform_instance_id_from_config = self.config.get("id") 
        if not platform_instance_id_from_config:
            platform_instance_id_from_config = f"vocechat_instance_{self.listen_port}"
            logger.warning(f"VoceChatAdapter: 未在配置中找到平台实例ID，使用自动生成的ID: {platform_instance_id_from_config}.")
        
        self.metadata = PlatformMetadata(name="vocechat", description="VoceChat 平台适配器", id=platform_instance_id_from_config)
        logger.info(f"VoceChatAdapter: 实例 '{self.metadata.id}' (类型: {self.metadata.name}) 初始化...")

        self._http_session: Optional[aiohttp.ClientSession] = None 
        self._webhook_runner: Optional[web.AppRunner] = None 
        self._webhook_site: Optional[web.TCPSite] = None    
        self._stop_event = asyncio.Event() 
        self._user_nickname_cache: Dict[str, str] = {} 

        if not self.server_url or not self.api_key: logger.error(f"VoceChatAdapter '{self.metadata.id}': `vocechat_server_url` 和 `api_key` 不能为空!")
        if self.default_bot_self_uid == "0" or self.default_bot_self_uid == "YOUR_BOT_USER_ID_IN_VOCECHAT":
             logger.warning(f"VoceChatAdapter '{self.metadata.id}': `default_bot_self_uid` 未配置或使用了默认占位符。")

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 创建新的 aiohttp.ClientSession。")
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    def meta(self) -> PlatformMetadata: return self.metadata 
    
    async def _handle_webhook_get_request(self, request: web.Request):
        logger.info(f"VoceChatAdapter '{self.metadata.id}': 收到 Webhook URL 验证 GET 请求: {request.path}")
        return web.Response(text="Webhook GET check OK", status=200)
    
    async def _handle_webhook_request(self, request: web.Request):
        try:
            raw_data = await request.json()
            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 收到 Webhook POST 数据: {json.dumps(raw_data, indent=2, ensure_ascii=False)}")
            abm = await self.convert_message(data=raw_data)
            if abm:
                platform_event = VoceChatEvent(message_obj=abm,platform_meta=self.meta(),adapter_instance=self)
                self.commit_event(platform_event)
                logger.debug(f"VoceChatAdapter '{self.metadata.id}': 已提交 VoceChatEvent 到事件队列。")
            else: 
                logger.warning(f"VoceChatAdapter '{self.metadata.id}': 无法转换消息或消息被忽略: {str(raw_data)[:500]}...")
            return web.Response(text="OK", status=200)
        except json.JSONDecodeError: 
            body_text = await request.text()
            logger.error(f"VoceChatAdapter '{self.metadata.id}': Webhook POST 数据非 JSON: {body_text[:500]}...")
            return web.Response(text="Invalid JSON", status=400)
        except Exception as e: 
            logger.error(f"VoceChatAdapter '{self.metadata.id}': Webhook POST 处理失败: {e}", exc_info=True)
            return web.Response(text="Internal Server Error", status=500)
            
    async def run(self):
        logger.info(f"VoceChatAdapter '{self.metadata.id}': 启动 Webhook 服务器，监听于 http://{self.listen_host}:{self.listen_port}{self.webhook_path}")
        app = web.Application()
        app.router.add_get(self.webhook_path, self._handle_webhook_get_request)
        app.router.add_post(self.webhook_path, self._handle_webhook_request)
        self._webhook_runner = web.AppRunner(app)
        await self._webhook_runner.setup()
        self._webhook_site = web.TCPSite(self._webhook_runner, self.listen_host, self.listen_port)
        try:
            await self._webhook_site.start()
            logger.info(f"VoceChatAdapter '{self.metadata.id}': Webhook 服务器已在 http://{self.listen_host}:{self.listen_port}{self.webhook_path} 运行。")
            await self._stop_event.wait() 
        except asyncio.CancelledError: 
            logger.info(f"VoceChatAdapter '{self.metadata.id}': Webhook 运行任务捕获到 CancelledError...")
        except Exception as e: 
            logger.error(f"VoceChatAdapter '{self.metadata.id}': Webhook 服务器错误: {e}", exc_info=True)
        finally: 
            logger.info(f"VoceChatAdapter '{self.metadata.id}': Webhook 服务器开始清理...")
            await self.shutdown_server_resources()
            logger.info(f"VoceChatAdapter '{self.metadata.id}': Webhook run 方法结束。")

    async def _fetch_user_nickname(self, user_id_str: str) -> str: 
        if not user_id_str or not user_id_str.strip() or not user_id_str.isdigit():
            logger.warning(f"VoceChatAdapter '{self.metadata.id}': 获取昵称失败，传入的 user_id_str ('{user_id_str}') 无效或非纯数字。")
            return f"VoceChatUser_{user_id_str if user_id_str and user_id_str.strip() else 'InvalidID'}"

        if user_id_str in self._user_nickname_cache:
            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 命中用户 {user_id_str} 的昵称缓存: {self._user_nickname_cache[user_id_str]}")
            return self._user_nickname_cache[user_id_str]
        
        default_nickname = f"VoceChatUser_{user_id_str}"
        
        if not self.get_user_nickname_from_api:
            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 配置未启用API获取昵称，用户 {user_id_str} 将使用默认昵称: {default_nickname}")
            return default_nickname
        
        logger.info(f"VoceChatAdapter '{self.metadata.id}': 准备调用API获取用户 {user_id_str} 的昵称 (配置已启用)。")

        try:
            user_id_int = int(user_id_str)

            # ★★★ 使用与 curl 命令中成功的 URL 格式 ★★★
            # 即 /api/bot/user/{uid}?uid={actual_id} 
            # (保持路径中的 {uid} 是字面量，实际的 user_id 通过查询参数传递)
            api_url = f"{self.server_url}/api/bot/user/{{uid}}?uid={user_id_int}"
            
            request_headers = {"x-api-key": self.api_key} 
            
            logger.info(f"VoceChatAdapter '{self.metadata.id}': API请求详情 (尝试混合格式) - URL: {api_url}, Headers: {{'x-api-key': '(已隐藏)'}}")
            
            http_client = await self._get_http_session()
            async with http_client.get(api_url, headers=request_headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                response_status = resp.status
                response_text = await resp.text() 
                logger.info(f"VoceChatAdapter '{self.metadata.id}': API响应 - Status: {response_status}, Raw Text (前500字符): {response_text[:500]}")
                
                if response_status == 200:
                    try:
                        user_info = json.loads(response_text)
                        logger.info(f"VoceChatAdapter '{self.metadata.id}': API成功响应 (已完整解析为JSON):\n{json.dumps(user_info, ensure_ascii=False, indent=2)}")
                        
                        retrieved_nickname: Optional[str] = None
                        
                        if user_info.get("name"): # ★★★ 优先尝试从顶层 'name' 字段获取 ★★★
                            retrieved_nickname = user_info["name"]
                            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 从 'name' 字段获取到昵称: '{retrieved_nickname}'")
                        elif user_info.get("user_detail") and isinstance(user_info["user_detail"], dict) and user_info["user_detail"].get("name"): 
                            retrieved_nickname = user_info["user_detail"]["name"]
                            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 从 user_detail.name 获取到昵称: '{retrieved_nickname}'")
                        elif user_info.get("username"): 
                            retrieved_nickname = user_info["username"]
                            logger.debug(f"VoceChatAdapter '{self.metadata.id}': 从 username 获取到昵称: '{retrieved_nickname}'")

                        if retrieved_nickname and retrieved_nickname.strip():
                            final_nickname = retrieved_nickname.strip()
                            self._user_nickname_cache[user_id_str] = final_nickname
                            logger.info(f"VoceChatAdapter '{self.metadata.id}': 成功获取并缓存用户 {user_id_str} 的昵称: '{final_nickname}'")
                            return final_nickname
                        else:
                            logger.info(f"VoceChatAdapter '{self.metadata.id}': API响应中未找到有效昵称字段，用户 {user_id_str} 将使用默认昵称（即使Status 200）。")
                            return default_nickname
                            
                    except json.JSONDecodeError as e_json_decode: 
                        logger.error(f"VoceChatAdapter '{self.metadata.id}': 获取用户 {user_id_str} (Status 200) 的响应无法解析为JSON: {e_json_decode}. Raw Text (前500): {response_text[:500]}"); 
                        return default_nickname
                else: 
                    logger.warning(f"VoceChatAdapter '{self.metadata.id}': 获取用户 {user_id_str} 昵称API请求失败: Status {response_status}. 响应体: {response_text[:500]}"); 
                    return default_nickname
        except asyncio.TimeoutError:
            logger.warning(f"VoceChatAdapter '{self.metadata.id}': API获取用户 {user_id_str} 昵称超时。")
            return default_nickname
        except aiohttp.ClientError as e_aio:
            logger.error(f"VoceChatAdapter '{self.metadata.id}': API获取用户 {user_id_str} 昵称时发生 aiohttp.ClientError: {e_aio}")
            return default_nickname
        except ValueError: # 理论上不应发生，因为前面有 isdigit() 检查
             logger.error(f"VoceChatAdapter '{self.metadata.id}': 用户ID '{user_id_str}' 无法转换为整数。")
             return default_nickname
        except Exception as e: 
            logger.error(f"VoceChatAdapter '{self.metadata.id}': API获取用户 {user_id_str} 昵称时发生未知异常: {e}", exc_info=True)
            return default_nickname

    async def convert_message(self, data: Dict[str, Any]) -> Optional[AstrBotMessage]:
        # ... (此方法与你上一个提供的版本一致，为了简洁省略了) ...
        # ... (请确保你使用的是包含图片处理、session_id正确设置的最新版本) ...
        abm = AstrBotMessage(); abm.message = [] 
        detail = data.get("detail", {}); content_from_detail = detail.get("content", ""); content_type = detail.get("content_type", "text/plain"); properties = detail.get("properties") 
        from_uid_str = str(data.get("from_uid") or ""); message_id_str = str(data.get("mid") or ""); target = data.get("target", {})
        if isinstance(content_from_detail, str) and content_from_detail.strip().lower() == "newuser": # 处理新用户加入事件
            actual_new_user_id_str = from_uid_str; nickname_for_new_user = f"NewUser_{from_uid_str}"
            if from_uid_str == '0' and isinstance(properties, dict) and "user" in properties: 
                new_user_info = properties.get("user")
                if isinstance(new_user_info, dict) and "uid" in new_user_info:
                    actual_new_user_id_str = str(new_user_info["uid"]); nickname_for_new_user = new_user_info.get("name", f"NewUser_{actual_new_user_id_str}")
            if actual_new_user_id_str and actual_new_user_id_str != '0' and actual_new_user_id_str != 'None': 
                abm.type = MessageType.SYSTEM_EVENT; abm.message.append(Plain(text=f"voce_new_user_event:{actual_new_user_id_str}")); 
                abm.user_id = actual_new_user_id_str; abm.nickname = nickname_for_new_user
                if "gid" in target: abm.group_id = str(target["gid"]) 
                abm.sender = MessageMember(user_id=abm.user_id, nickname=abm.nickname); abm.message_str = "新用户加入"; 
                abm.raw_message = data; abm.self_id = self.default_bot_self_uid
                abm.session_id = abm.group_id if abm.group_id else abm.user_id; # session_id 可能是群ID或用户ID
                abm.message_id = message_id_str if message_id_str and message_id_str != 'None' else str(data.get("created_at", uuid.uuid4())); return abm
            else: logger.warning(f"VoceChatAdapter '{self.metadata.id}': newuser事件, 无法确定用户ID..."); return None # 无法确定用户ID，忽略此事件
        if not from_uid_str or from_uid_str == 'None' or from_uid_str == '0': logger.warning(f"VoceChatAdapter '{self.metadata.id}': 无效发送者ID ('{from_uid_str}')."); return None
        abm.user_id = from_uid_str; abm.nickname = await self._fetch_user_nickname(from_uid_str); abm.sender = MessageMember(user_id=abm.user_id, nickname=abm.nickname)
        abm.message_id = message_id_str if message_id_str and message_id_str != 'None' else str(uuid.uuid4())
        if content_type == "text/plain" or content_type == "text/markdown": abm.message_str = str(content_from_detail); abm.message.append(Plain(text=str(content_from_detail)))
        elif content_type == "vocechat/file":
            file_path_from_vocechat = content_from_detail ; file_name = "file"; actual_mime_type = "application/octet-stream"
            if isinstance(properties, dict): 
                if "files" in properties and isinstance(properties["files"], list) and len(properties["files"]) > 0: 
                    file_info = properties["files"][0]; file_name = file_info.get("name", file_name); actual_mime_type = file_info.get("content_type", actual_mime_type)
                    if "path" in file_info and not file_path_from_vocechat: file_path_from_vocechat = file_info["path"]
                else: file_name = properties.get("name", file_name); actual_mime_type = properties.get("content_type", actual_mime_type)
            logger.info(f"VoceChat '{self.metadata.id}': 文件元数据: name='{file_name}', type='{actual_mime_type}'")
            abm.message_str = f"[{file_name}]" # 即使是图片，message_str也只是占位符
            if file_path_from_vocechat and isinstance(file_path_from_vocechat, str):
                encoded_file_path = quote_plus(file_path_from_vocechat); file_api_url = f"{self.server_url}/api/resource/file?file_path={encoded_file_path}"
                if actual_mime_type.startswith("image/"):
                    logger.debug(f"VoceChat '{self.metadata.id}': 检测到图片 '{file_name}'. 从 {file_api_url} 下载")
                    try:
                        http_client = await self._get_http_session()
                        async with http_client.get(file_api_url, headers={"x-api-key": self.api_key}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            if resp.status == 200:
                                image_bytes = await resp.read(); base64_data_str = base64.b64encode(image_bytes).decode('utf-8')
                                image_component = Image.fromBase64(base64_data_str) # 使用 AstrBot 的工厂方法
                                abm.message.append(image_component); logger.info(f"VoceChat '{self.metadata.id}': 已为 '{file_name}' 创建 Image 组件 (Base64)。")
                            else: err_text = await resp.text(); logger.error(f"VoceChat '{self.metadata.id}': 图片下载失败 '{file_name}': {resp.status} - {err_text[:200]}"); abm.message.append(Plain(text=f"[图片下载失败: {file_name}]"))
                    except asyncio.TimeoutError: logger.warning(f"VoceChat '{self.metadata.id}': 下载图片 '{file_name}' 超时。"); abm.message.append(Plain(text=f"[图片下载超时: {file_name}]"))
                    except Exception as e_img: logger.error(f"VoceChat '{self.metadata.id}': 图片处理异常 '{file_name}': {e_img}", exc_info=True); abm.message.append(Plain(text=f"[图片处理异常: {file_name}]"))
                else: abm.message.append(Plain(text=f"收到文件: {file_name}")); logger.info(f"VoceChat '{self.metadata.id}': 收到非图片文件 '{file_name}'.")
            else: logger.warning(f"VoceChat '{self.metadata.id}': 文件消息路径无效。属性: {properties}"); abm.message.append(Plain(text=f"[文件路径无效: {file_name}]"))
        else:
            abm.message_str = str(content_from_detail) if content_from_detail else "[未知类型]"; 
            logger.warning(f"VoceChat '{self.metadata.id}': 未知 content_type: {content_type} for '{str(content_from_detail)[:50]}...'"); abm.message.append(Plain(text=str(content_from_detail))) 
        if "gid" in target and target["gid"] is not None: abm.type = MessageType.GROUP_MESSAGE; abm.group_id = str(target["gid"]); abm.session_id = abm.group_id;
        elif "uid" in target and target["uid"] is not None: abm.type = MessageType.FRIEND_MESSAGE; abm.session_id = abm.user_id; # 私聊时，session_id为消息发送方ID
        else: logger.warning(f"VoceChatAdapter '{self.metadata.id}': target未知({target})，根据from_uid({abm.user_id})默认为私聊."); abm.type = MessageType.FRIEND_MESSAGE; abm.session_id = abm.user_id;
        abm.self_id = self.default_bot_self_uid ; abm.raw_message = data; 
        if not abm.message: logger.debug(f"VoceChatAdapter '{self.metadata.id}': 最终消息列表为空 for mid {message_id_str}."); return None
        return abm
        
    async def send_by_session(self, session: MessageSesion, message_chain: MessageChain):
        # ... (此方法与你上一个提供的版本一致，为了简洁省略了) ...
        # ... (请确保你使用的是包含图片发送逻辑的最新版本) ...
        target_id_str = ""; api_path_segment = "";
        if session.message_type == MessageType.FRIEND_MESSAGE: 
            if not session.session_id: logger.error(f"'{self.metadata.id}': 私聊 session.session_id 为空!"); return
            target_id_str = session.session_id; api_path_segment = f"/api/bot/send_to_user/{target_id_str}"
        elif session.message_type == MessageType.GROUP_MESSAGE: 
            if not session.session_id: logger.error(f"'{self.metadata.id}': 群聊 session.session_id 为空!"); return
            target_id_str = session.session_id; api_path_segment = f"/api/bot/send_to_group/{target_id_str}"
        else: logger.error(f"VoceChatAdapter '{self.metadata.id}': send_by_session 不支持类型: {session.message_type}"); return
        send_url_base = f"{self.server_url}{api_path_segment}"; components_to_send = []
        if hasattr(message_chain, 'chain') and isinstance(message_chain.chain, list): components_to_send = message_chain.chain
        elif isinstance(message_chain, list): components_to_send = message_chain 
        elif isinstance(message_chain, str): components_to_send = [Plain(text=message_chain)]
        else: logger.error(f"VoceChatAdapter '{self.metadata.id}': message_chain 类型无法处理: {type(message_chain)}"); return
        http_client = await self._get_http_session()
        for component_index, component in enumerate(components_to_send): 
            request_headers = {"x-api-key": self.api_key}; data_to_send: Any = None; send_url = send_url_base
            comp_desc = f"Comp#{component_index+1} Type:{type(component).__name__}"
            try:
                if isinstance(component, Plain):
                    content_to_send = component.text
                    if self.send_plain_as_markdown: request_headers["Content-Type"] = "text/markdown"; desc_type = "Markdown"
                    else: request_headers["Content-Type"] = "text/plain"; desc_type = "Plain"
                    data_to_send = content_to_send.encode('utf-8')
                    logger.debug(f"VoceChat '{self.metadata.id}': 发送 {desc_type} '{content_to_send[:50]}...' 到 {target_id_str} ({comp_desc})")
                elif isinstance(component, Image):
                    if component.file and component.file.startswith("base64://"):
                        pure_base64_data = component.file.split("base64://", 1)[1]; mime_type = "image/png" ; original_filename = "image.png" # 默认值
                        if hasattr(component, 'path') and component.path: original_filename = os.path.basename(component.path)
                        guessed_mime, _ = mimetypes.guess_type(original_filename)
                        if guessed_mime: mime_type = guessed_mime
                        data_url_for_voce = f"data:{mime_type};base64,{pure_base64_data}"
                        request_headers["Content-Type"] = "vocechat/file"; data_to_send = json.dumps({"archive_id": data_url_for_voce }).encode('utf-8')
                        logger.debug(f"VoceChat '{self.metadata.id}': 发送图片 (vocechat/file, archive_id) 到 {target_id_str} ({comp_desc})")
                    elif component.url and component.url.startswith("http"): 
                        logger.debug(f"VoceChat '{self.metadata.id}': 发送图片 (Markdown链接: ![]({component.url[:100]}...)) 到 {target_id_str} ({comp_desc})")
                        request_headers["Content-Type"] = "text/markdown"; data_to_send = f"![]({component.url})".encode('utf-8')
                    else: logger.warning(f"'{self.metadata.id}' Image组件无有效file(base64://)或url(http). ({comp_desc})"); continue
                else: logger.warning(f"VoceChatAdapter '{self.metadata.id}': 不支持的发送组件类型: {comp_desc}"); continue
                async with http_client.post(send_url, headers=request_headers, data=data_to_send, timeout=aiohttp.ClientTimeout(total=10)) as resp: 
                    response_text = await resp.text()
                    if resp.status == 200 or resp.status == 201: # 201 Created 也是成功
                        try: response_data = json.loads(response_text); logger.info(f"'{self.metadata.id}' 消息发送到:{target_id_str} 成功: {response_data} ({comp_desc})")
                        except json.JSONDecodeError: logger.info(f"'{self.metadata.id}' 消息发送到:{target_id_str} 成功(非JSON响应): {response_text[:100]}... ({comp_desc})")
                    else: logger.error(f"'{self.metadata.id}' 消息发送到:{target_id_str} 失败 ({request_headers.get('Content-Type')}): {resp.status} - {response_text[:200]}... ({comp_desc})")
            except asyncio.TimeoutError: logger.error(f"'{self.metadata.id}' 发送消息到 {target_id_str} 超时 ({comp_desc}).")
            except aiohttp.ClientError as e_aio_send: logger.error(f"'{self.metadata.id}' 发送消息时网络错误({comp_desc}): {e_aio_send}")
            except Exception as e_send: logger.error(f"'{self.metadata.id}' 未知发送错误 ({comp_desc}): {e_send}", exc_info=True)

    async def shutdown_server_resources(self):
        if self._webhook_site and hasattr(self._webhook_site, '_server') and self._webhook_site._server is not None : 
            logger.info(f"VoceChatAdapter '{self.metadata.id}': 停止Webhook site...")
            await self._webhook_site.stop(); self._webhook_site = None
        if self._webhook_runner:
            logger.info(f"VoceChatAdapter '{self.metadata.id}': 清理Webhook AppRunner...")
            await self._webhook_runner.cleanup(); self._webhook_runner = None
        if self._http_session and not self._http_session.closed:
            logger.info(f"VoceChatAdapter '{self.metadata.id}': 关闭 aiohttp session...")
            await self._http_session.close(); self._http_session = None

    async def shutdown(self): 
        logger.info(f"VoceChatAdapter '{self.metadata.id}': 开始执行 shutdown...")
        self._stop_event.set(); await asyncio.sleep(0.1) 
        await self.shutdown_server_resources()
        logger.info(f"VoceChatAdapter '{self.metadata.id}': Shutdown 完成。")

